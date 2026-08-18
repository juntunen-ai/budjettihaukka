# Alamomentin semanttinen malli

## Kenttien merkitys

- Lähteen `TakpMrL_Tunnus` ja `TakpMrL_sNimi` ovat määrärahalajin koodi ja nimi. Semanttisessa mallissa niiden nimet ovat `maararahalaji_tunnus` ja `maararahalaji_snimi`.
- Lähteen `TakpT_TunnusP` ja `TakpT_sNimi` ovat talousarviotilin koko koodi ja nimi. Semanttisessa mallissa niiden nimet ovat `talousarviotili_tunnusp` ja `talousarviotili_snimi`.
- Määrärahalajia ei saa käyttää alamomentin tunnuksena tai nimenä.

## Dokumentoitu johtosääntö

Alamomenttiehdokas johdetaan vain, kun kaikki ehdot täyttyvät:

1. Momentin koodi vastaa muotoa `NN.NN.NN.`.
2. Talousarviotilin koodi vastaa muotoa `NN.NN.NN.<numero>[.<numero>...].`.
3. Talousarviotilin koodi alkaa täsmälleen saman rivin momentin koodilla.
4. Talousarviotilin koodi on momenttia pidempi.

Ehdokkaan tunnus on momentin koodin jälkeinen osa. Esimerkiksi momentista `27.10.01.` ja talousarviotilistä `27.10.01.9.01.` johdetaan ehdokas `9.01.`. Ehdokkaan nimeksi otetaan talousarviotilin nimi.

Koodisääntö tuottaa vain `alamomentti_tunnus_candidate`- ja `alamomentti_snimi_candidate`-kentät. Se ei yksin oikeuta julkaisemaan alamomenttia.

## Virallinen validointi

`dim_alamomentti` hyväksyy ehdokkaan vain, jos `official_code_registry_v1` sisältää samalle vuodelle täsmälleen saman koko talousarviotilin koodin tasolla `talousarviotili` tai `alamomentti`. Semanttisen näkymän varsinaiset `alamomentti_tunnus`- ja `alamomentti_snimi`-kentät tulevat ainoastaan tästä validoidusta dimensiosta.

Validointitila on yksi seuraavista:

- `not_derivable`: koodin rakenne tai momenttisuhde ei täytä johtosääntöä;
- `not_in_official_chart`: ehdokas syntyi, mutta vuosikohtaista virallista osumaa ei ole;
- `validated`: vuosikohtainen virallinen osuma löytyi.

Nykyinen virallinen rekisteri sisältää vain pääluokka-, luku- ja momenttitasot. Siksi alamomenttikyselyt ovat fail-closed-periaatteella pois käytöstä. Sovellus palauttaa tilan `unsupported_entity_level` eikä muodosta tai suorita SQL:ää.

## Käyttöönoton purkukriteerit

Alamomenttikyselyt voidaan ottaa käyttöön vasta, kun:

1. vuosikohtainen virallinen talousarvion tilijaottelu on ladattu rekisteriin;
2. ehdokkaan koko talousarviotilikoodi validoituu tarkalla koodi- ja vuosiosumalla;
3. kattavuus, duplikaatit ja nimi-konfliktit on raportoitu;
4. regressiotestit osoittavat, ettei yksikään määrärahalaji päädy alamomentiksi;
5. alamomenttikyselyjen golden- ja runtime-testit läpäisevät julkaisuportin.
