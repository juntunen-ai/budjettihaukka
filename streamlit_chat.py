"""Budjettihaukka — conversational UI (Milestone B MVP).

Chat thread over the agent engine: ask in any language, get an answer with
its definition/uncertainty context, continue with follow-ups.

Run: streamlit run streamlit_chat.py --server.port 8502
"""

import logging

import streamlit as st

from config import settings
from services.agent_engine import BudgetAgent

logging.basicConfig(level=logging.WARNING)

st.set_page_config(page_title="Budjettihaukka — keskustelu", page_icon="🦅", layout="centered")

st.title("🦅 Budjettihaukka · keskustelu (beta)")
st.caption(
    "Kysy valtion budjetista millä kielellä tahansa. Vastaukset perustuvat "
    "Valtiokonttorin kirjanpitoaineistoon 1998–2026 ja ihmisen vahvistamiin "
    "käsitemääritelmiin; agentti kertoo aina miten luvut on laskettu ja mitä "
    "epävarmuuksia niihin liittyy."
)

if "agent" not in st.session_state:
    try:
        st.session_state.agent = BudgetAgent()
    except RuntimeError as error:
        st.error(str(error))
        st.stop()
if "messages" not in st.session_state:
    st.session_state.messages = []
if "pending_question" not in st.session_state:
    st.session_state.pending_question = None

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


def _queue_question(text: str) -> None:
    st.session_state.pending_question = text


typed = st.chat_input("Kysy budjetista… / Ask about the budget…")
question = typed or st.session_state.pending_question
st.session_state.pending_question = None

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)
    with st.chat_message("assistant"):
        with st.spinner("Haukka tutkii budjettia…"):
            turn = st.session_state.agent.ask(question)
        if turn.error:
            st.error(turn.error)
            st.session_state.messages.append(
                {"role": "assistant", "content": f"⚠️ {turn.error}"}
            )
        else:
            st.markdown(turn.answer)
            st.session_state.messages.append({"role": "assistant", "content": turn.answer})
            meta_bits = [f"malli: {st.session_state.agent.active_model}"]
            if turn.tool_calls:
                meta_bits.append("työkalut: " + ", ".join(turn.tool_calls))
            meta_bits.append(f"{turn.elapsed_s:.1f} s")
            st.caption(" · ".join(meta_bits))
            if turn.follow_ups:
                cols = st.columns(len(turn.follow_ups))
                for col, follow_up in zip(cols, turn.follow_ups):
                    col.button(
                        follow_up,
                        on_click=_queue_question,
                        args=(follow_up,),
                        use_container_width=True,
                    )

with st.sidebar:
    st.markdown("**Tietoa**")
    st.markdown(
        "- Data: Valtiokonttori, kuukausikirjanpito 1998–2026/05\n"
        "- Käsitteet: ihmisen vahvistamat määritelmät (esim. koulutus v1)\n"
        "- Kaikki SQL kulkee turvaportin läpi (vain SELECT, taulu-whitelist, kustannuskatto)\n"
        f"- Malli: {settings.gemini_model} (automaattinen varamalli ruuhkassa)"
    )
    if st.button("Tyhjennä keskustelu"):
        st.session_state.messages = []
        st.session_state.agent = BudgetAgent()
        st.rerun()
