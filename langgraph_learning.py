# my_langgraph_learning.py

import os
import json
import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
import logging
import hashlib

# Määritetään lokitus
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("agent_learning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("langgraph_learning")

class AgentLearningSystem:
    """Luokka, joka hallinnoi agentin oppimista ja parantamista."""
    
    def __init__(self, data_dir: str = "agent_data"):
        """
        Alustaa oppimismoottorin.
        
        Args:
            data_dir: Hakemisto, johon oppimisdata tallennetaan
        """
        self.data_dir = data_dir
        self.questions_file = os.path.join(data_dir, "questions.json")
        self.examples_file = os.path.join(data_dir, "successful_examples.json")
        self.failure_file = os.path.join(data_dir, "failure_cases.json")
        self.patterns_file = os.path.join(data_dir, "success_patterns.json")
        
        # Luo tietorakenteet
        os.makedirs(data_dir, exist_ok=True)
        
        # Lataa olemassa olevat tiedot
        self.questions = self._load_json(self.questions_file, [])
        self.successful_examples = self._load_json(self.examples_file, [])
        self.failure_cases = self._load_json(self.failure_file, [])
        self.success_patterns = self._load_json(self.patterns_file, {})
    
    def _load_json(self, filepath: str, default: Any) -> Any:
        """Lataa JSON-tiedosto tai palauttaa oletusarvon, jos tiedostoa ei ole."""
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Virhe ladattaessa tiedostoa {filepath}: {e}")
                return default
        return default
    
    def _save_json(self, filepath: str, data: Any) -> None:
        """Tallentaa datan JSON-tiedostoon."""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except IOError as e:
            logger.error(f"Virhe tallennettaessa tiedostoon {filepath}: {e}")
    
    def _calculate_hash(self, question: str) -> str:
        """Laskee kysymykselle uniikin tunnisteen."""
        return hashlib.md5(question.lower().strip().encode('utf-8')).hexdigest()
    
    def record_interaction(self, question: str, result: Dict, feedback: Optional[Dict] = None) -> str:
        """
        Tallentaa vuorovaikutuksen oppimista varten.
        
        Args:
            question: Käyttäjän kysymys
            result: Agentin tuottama vastaus ja metatiedot
            feedback: Valinnainen käyttäjän palaute (esim. "peukalo ylös/alas")
            
        Returns:
            str: Vuorovaikutuksen tunniste
        """
        # Luo vuorovaikutuksen tunniste
        interaction_id = self._calculate_hash(question)
        timestamp = datetime.datetime.now().isoformat()
        
        # Rakenna vuorovaikutusdata
        interaction_data = {
            "id": interaction_id,
            "timestamp": timestamp,
            "question": question,
            "sql_query": result.get("sql_query", ""),
            "final_answer": result.get("answer", ""),
            "execution_steps": result.get("execution_steps", []),
            "success": "error_message" not in result or not result.get("error_message"),
            "feedback": feedback or {}
        }
        
        # Tallenna kysymys historiaan
        existing_indices = [i for i, q in enumerate(self.questions) 
                           if q.get("id") == interaction_id]
        
        if existing_indices:
            # Päivitä olemassa oleva kysymys
            self.questions[existing_indices[0]] = interaction_data
        else:
            # Lisää uusi kysymys
            self.questions.append(interaction_data)
        
        # Tallenna onnistuneet esimerkit tai epäonnistumiset
        if interaction_data["success"]:
            # Onnistuneen tapauksen tallennus
            existing_success = [i for i, q in enumerate(self.successful_examples) 
                               if q.get("id") == interaction_id]
            if existing_success:
                self.successful_examples[existing_success[0]] = interaction_data
            else:
                self.successful_examples.append(interaction_data)
                
            # Analysoi onnistumisen kaava
            self._analyze_success_pattern(interaction_data)
        else:
            # Epäonnistuneen tapauksen tallennus
            existing_failure = [i for i, q in enumerate(self.failure_cases) 
                               if q.get("id") == interaction_id]
            if existing_failure:
                self.failure_cases[existing_failure[0]] = interaction_data
            else:
                self.failure_cases.append(interaction_data)
        
        # Tallenna päivitetyt tiedot
        self._save_json(self.questions_file, self.questions)
        self._save_json(self.examples_file, self.successful_examples)
        self._save_json(self.failure_file, self.failure_cases)
        self._save_json(self.patterns_file, self.success_patterns)
        
        return interaction_id
    
    def _analyze_success_pattern(self, interaction: Dict) -> None:
        """Analysoi onnistuneen vuorovaikutuksen kaava oppimista varten."""
        # Etsi SQL-kyselyn rakenteellisia ominaisuuksia
        sql = interaction.get("sql_query", "").lower()
        
        # Tunnista käytetyt SQL-rakenteet
        patterns = {
            "has_join": "join" in sql,
            "has_group_by": "group by" in sql,
            "has_order_by": "order by" in sql,
            "has_where": "where" in sql,
            "has_having": "having" in sql,
            "has_limit": "limit" in sql,
            "has_subquery": "select" in sql[sql.find("select")+6:] if "select" in sql else False,
            "has_with": sql.strip().startswith("with"),
            "has_case": "case" in sql,
            "has_date_funcs": any(func in sql for func in ["date", "extract", "year", "month", "day"]),
        }
        
        # Päivitä tilastoja
        for pattern, exists in patterns.items():
            if exists:
                if pattern not in self.success_patterns:
                    self.success_patterns[pattern] = {"count": 0, "examples": []}
                
                self.success_patterns[pattern]["count"] += 1
                
                # Säilytä vain muutama esimerkki kustakin kaavasta
                examples = self.success_patterns[pattern]["examples"]
                if interaction["id"] not in [ex.get("id") for ex in examples]:
                    if len(examples) >= 5:  # Rajoita esimerkkien määrää
                        examples.pop(0)
                    examples.append({
                        "id": interaction["id"],
                        "question": interaction["question"],
                        "sql_query": interaction["sql_query"]
                    })
    
    def get_similar_questions(self, question: str, limit: int = 3) -> List[Dict]:
        """
        Hakee samankaltaisia kysymyksiä oppimisdatasta.
        
        Todellisessa toteutuksessa tämä käyttäisi vektoriembeddingjä tai muuta 
        semanttista vastaavuutta, mutta tässä käytämme yksinkertaista avainsanojen vastaavuutta.
        
        Args:
            question: Kysymys, johon halutaan löytää vastaavia
            limit: Palautettavien tulosten määrä
            
        Returns:
            List[Dict]: Lista samankaltaisia kysymyksiä ja niiden vastauksia
        """
        # Yksinkertainen avainsanateknilkka
        question_words = set(question.lower().split())
        
        # Laske samankaltaisuus jokaiselle tallennetulle kysymykselle
        similarities = []
        for q in self.questions:
            q_words = set(q["question"].lower().split())
            common_words = question_words.intersection(q_words)
            if common_words:
                similarity = len(common_words) / (len(question_words) + len(q_words) - len(common_words))
                similarities.append((similarity, q))
        
        # Palauta samankaltaisimmat kysymykset
        similarities.sort(reverse=True)
        return [q for _, q in similarities[:limit]]
    
    def get_success_patterns(self) -> Dict:
        """Palauttaa löydetyt onnistumiskaavat."""
        return self.success_patterns
    
    def get_common_errors(self) -> Dict:
        """Analysoi ja palauttaa yleisimmät virhetyypit."""
        error_types = {}
        
        for case in self.failure_cases:
            error_msg = case.get("error_message", "")
            
            # Yksinkertainen luokittelu
            error_type = "unknown"
            if "syntax error" in error_msg.lower():
                error_type = "syntax_error"
            elif "column not found" in error_msg.lower() or "no such column" in error_msg.lower():
                error_type = "column_not_found"
            elif "table not found" in error_msg.lower() or "no such table" in error_msg.lower():
                error_type = "table_not_found"
            elif "permission denied" in error_msg.lower():
                error_type = "permission_error"
            elif "timeout" in error_msg.lower():
                error_type = "timeout"
            
            # Kasvata laskuria
            if error_type not in error_types:
                error_types[error_type] = {"count": 0, "examples": []}
            
            error_types[error_type]["count"] += 1
            
            # Tallenna esimerkkejä
            examples = error_types[error_type]["examples"]
            if len(examples) < 3:  # Rajoita esimerkkien määrää
                examples.append({
                    "question": case["question"],
                    "sql_query": case.get("sql_query", ""),
                    "error_message": error_msg
                })
        
        return error_types
    
    def generate_improvement_recommendations(self) -> Dict:
        """
        Tuottaa suosituksia järjestelmän parantamiseksi perustuen analyysiin.
        
        Returns:
            Dict: Suositukset eri kategorioissa
        """
        # Analysoi eri näkökulmia
        error_patterns = self.get_common_errors()
        success_patterns = self.get_success_patterns()
        
        # Laske perustilastot
        total_questions = len(self.questions)
        success_count = len(self.successful_examples)
        failure_count = len(self.failure_cases)
        
        if total_questions == 0:
            return {"message": "Ei riittävästi dataa analyysiä varten"}
        
        success_rate = success_count / total_questions if total_questions > 0 else 0
        
        # Luo suositukset
        recommendations = {
            "overall_stats": {
                "total_questions": total_questions,
                "success_rate": success_rate,
                "success_count": success_count,
                "failure_count": failure_count
            },
            "sql_pattern_recommendations": [],
            "error_handling_recommendations": [],
            "prompt_improvement_recommendations": []
        }
        
        # SQL-kaavoihin perustuvat suositukset
        for pattern, data in success_patterns.items():
            pattern_success_rate = data["count"] / success_count if success_count > 0 else 0
            if pattern_success_rate > 0.7:  # Jos kaava on yleinen onnistuneissa tapauksissa
                recommendations["sql_pattern_recommendations"].append({
                    "pattern": pattern,
                    "suggestion": f"Suosi tätä SQL-rakennetta ({pattern}) prompteissa, koska se on yleinen onnistuneissa tapauksissa",
                    "examples": data["examples"][:2]  # Rajoita esimerkkien määrää
                })
        
        # Virheenkäsittelysuositukset
        for error_type, data in error_patterns.items():
            if data["count"] >= 3:  # Jos virhe on toistuva
                recommendations["error_handling_recommendations"].append({
                    "error_type": error_type,
                    "suggestion": f"Paranna käsittelyä virhetyyppille: {error_type}",
                    "frequency": data["count"],
                    "examples": data["examples"][:2]  # Rajoita esimerkkien määrää
                })
        
        # Prompt-parannusehdotukset
        if len(error_patterns) > 0:
            common_errors = sorted(error_patterns.items(), key=lambda x: x[1]["count"], reverse=True)
            most_common_error = common_errors[0][0]
            recommendations["prompt_improvement_recommendations"].append({
                "focus_area": "error_prevention",
                "suggestion": f"Lisää promptiin ohjeita välttämään virhetyyppiä: {most_common_error}",
                "rationale": f"Tämä virhetyyppi esiintyy useimmin ({common_errors[0][1]['count']} kertaa)"
            })
        
        # Jos jokin kysymystyyppi toistuu usein
        common_words = {}
        for q in self.questions:
            for word in q["question"].lower().split():
                if len(word) > 3:  # Vain yli 3 merkin sanat
                    common_words[word] = common_words.get(word, 0) + 1
        
        frequent_terms = sorted(common_words.items(), key=lambda x: x[1], reverse=True)[:5]
        if frequent_terms:
            recommendations["prompt_improvement_recommendations"].append({
                "focus_area": "query_optimization",
                "suggestion": f"Optimoi prompti käsittelemään termejä: {', '.join([term for term, _ in frequent_terms])}",
                "rationale": "Nämä termit esiintyvät usein käyttäjien kysymyksissä"
            })
        
        return recommendations
    
    def export_learning_data(self, format: str = "json") -> str:
        """
        Vie oppimisdatan haluttuun formaattiin.
        
        Args:
            format: Viennin formaatti ('json' tai 'csv')
            
        Returns:
            str: Polku vientitiedostoon
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format.lower() == "json":
            export_data = {
                "questions": self.questions,
                "successful_examples": self.successful_examples,
                "failure_cases": self.failure_cases,
                "success_patterns": self.success_patterns,
                "export_timestamp": timestamp,
                "stats": {
                    "total_questions": len(self.questions),
                    "success_count": len(self.successful_examples),
                    "failure_count": len(self.failure_cases)
                }
            }
            
            export_file = os.path.join(self.data_dir, f"learning_export_{timestamp}.json")
            self._save_json(export_file, export_data)
            return export_file
            
        elif format.lower() == "csv":
            # Vie kysymykset CSV-tiedostoon
            export_file = os.path.join(self.data_dir, f"learning_export_{timestamp}.csv")
            
            # Muunna tiedot pandas-kehykseksi
            records = []
            for q in self.questions:
                record = {
                    "id": q.get("id", ""),
                    "timestamp": q.get("timestamp", ""),
                    "question": q.get("question", ""),
                    "success": q.get("success", False),
                    "sql_query": q.get("sql_query", "")
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            df.to_csv(export_file, index=False, encoding='utf-8')
            return export_file
            
        else:
            raise ValueError(f"Tuntematon vientimuoto: {format}")

# Esimerkki käytöstä
def main():
    """Esimerkki oppimistoiminnon käytöstä."""
    # Luo oppimismoottori
    learning = AgentLearningSystem()
    
    # Simuloi muutamia vuorovaikutuksia
    examples = [
        {
            "question": "Paljonko opetusministeriöllä oli määrärahoja vuonna 2022?",
            "result": {
                "answer": "Opetusministeriöllä oli määrärahoja 6.7 miljardia euroa vuonna 2022.",
                "sql_query": "SELECT SUM(maararaha) FROM `valtion-budjetti-data.valtiodata.budjettidata` WHERE ministeriö = 'Opetusministeriö' AND vuosi = 2022",
                "execution_steps": ["Kysymys analysoitu", "SQL luotu", "Kysely suoritettu", "Vastaus muotoiltu"]
            },
            "feedback": {"thumbs_up": True}
        },
        {
            "question": "Mitkä ministeriöt saivat eniten rahoitusta vuosina 2020-2023?",
            "result": {
                "answer": "Eniten rahoitusta saivat: 1. Valtiovarainministeriö (45.3 mrd), 2. Sosiaali- ja terveysministeriö (32.1 mrd), 3. Opetusministeriö (26.8 mrd).",
                "sql_query": "SELECT ministeriö, SUM(maararaha) as summa FROM `valtion-budjetti-data.valtiodata.budjettidata` WHERE vuosi BETWEEN 2020 AND 2023 GROUP BY ministeriö ORDER BY summa DESC LIMIT 5",
                "execution_steps": ["Kysymys analysoitu", "SQL luotu", "Kysely suoritettu", "Vastaus muotoiltu"]
            },
            "feedback": {"thumbs_up": True}
        },
        {
            "question": "Miten kulttuurin määrärahat ovat muuttuneet vuosina 2010-2022?",
            "result": {
                "error_message": "Column 'kulttuuri' not found in table `valtion-budjetti-data.valtiodata.budjettidata`",
                "sql_query": "SELECT vuosi, SUM(maararaha) as summa FROM `valtion-budjetti-data.valtiodata.budjettidata` WHERE luokka = 'kulttuuri' GROUP BY vuosi ORDER BY vuosi",
                "execution_steps": ["Kysymys analysoitu", "SQL luotu", "Kysely epäonnistui"]
            },
            "feedback": {"thumbs_up": False}
        }
    ]
    
    # Tallenna esimerkit
    for example in examples:
        learning.record_interaction(example["question"], example["result"], example["feedback"])
    
    # Hae samankaltaisia kysymyksiä
    similar = learning.get_similar_questions("Mitkä ministeriöt saivat eniten rahaa vuonna 2022?")
    print("Samankaltaiset kysymykset:")
    for q in similar:
        print(f"- {q['question']}")
    
    # Hae parannussuosituksia
    recommendations = learning.generate_improvement_recommendations()
    print("\nSuositukset järjestelmän parantamiseksi:")
    print(f"- Onnistumisprosentti: {recommendations['overall_stats']['success_rate']*100:.1f}%")
    
    if recommendations["prompt_improvement_recommendations"]:
        for rec in recommendations["prompt_improvement_recommendations"]:
            print(f"- {rec['suggestion']} ({rec['rationale']})")
    
    # Vie oppimisdata
    export_file = learning.export_learning_data()
    print(f"\nOppimisdata viety tiedostoon: {export_file}")

if __name__ == "__main__":
    main()