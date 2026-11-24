import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from urllib.parse import unquote

from genera_archivio import genera_da_cartelle, prepare_messages


class AttachmentTests(unittest.TestCase):
    def test_normalizza_audio_annidato_e_riferimenti_dei_componenti(self):
        marker = "\ue200filecite\ue202turn0file1\ue201"
        raw = {"current_node": "one", "mapping": {"one": {"message": {
            "author": {"role": "user"},
            "content": {"parts": [
                {"content_type": "real_time_user_audio_video_asset_pointer", "audio_asset_pointer": {
                    "content_type": "audio_asset_pointer", "asset_pointer": "sediment://file-voce",
                    "format": "wav",
                }},
                {"content_type": "audio_transcription", "text": "Trascrizione"},
            ]},
            "metadata": {
                "content_references": [
                    {"type": "file", "matched_text": marker, "id": "file-documento", "name": "Documento.pdf"},
                    {"type": "video", "matched_text": "\ue200video\ue202ref\ue201", "url": "https://example.test/video"},
                ],
                "model_dil_v2": {"appData": {"opGenui": {"componentResults": {"one": {
                    "componentName": "AsyncImage",
                    "state": {"images": [{"url": "https://example.test/immagine", "image_search_query": "foto"}]},
                }}}}},
            },
        }}}}
        messages, refs = prepare_messages(raw, "")
        self.assertEqual(messages[0]["parts"], [{"ref": "file_voce"}, "Trascrizione"])
        self.assertEqual(refs["file_voce"]["mime"], "audio/wav")
        self.assertEqual(messages[0]["sources"][0]["ref"], "file_documento")
        self.assertEqual(messages[0]["components"][0]["links"][0]["url"], "https://example.test/immagine")

    def test_conserva_la_posizione_dei_file_e_i_metadati_del_blocco(self):
        raw = {"current_node": "msg", "mapping": {"msg": {"message": {
            "author": {"role": "assistant"},
            "content": {"parts": [
                "Prima",
                {"asset_pointer": "sediment://file_immagine", "content_type": "image_asset_pointer"},
                "Dopo\n<WritingBlock id='scrittura'/>",
            ]},
            "metadata": {
                "attachments": [{"id": "file-assente", "name": "Documento.pdf"}, {"id": "user-legacy"}],
                "writing_blocks": {"scrittura": {"subject": "Oggetto", "content": "Corpo salvato"}},
            },
        }}}}
        messages, refs = prepare_messages(raw, "")
        self.assertEqual(messages[0]["parts"], [
            "Prima", {"ref": "file_immagine"}, "Dopo\n<WritingBlock id='scrittura'/>",
        ])
        self.assertEqual(messages[0]["refs"], [
            "file_immagine", "file_assente", "riferimento:user-legacy", "Canvas_scrittura.md",
        ])
        self.assertEqual(refs["file_assente"]["id"], "file-assente")
        self.assertEqual(refs["riferimento:user-legacy"]["source_id"], "user-legacy")
        self.assertEqual(messages[0]["blocks"], [{
            "id": "scrittura", "title": "Oggetto", "text": "Corpo salvato", "ref": "Canvas_scrittura.md",
        }])

    def test_recupera_id_nell_export_e_limita_il_ripiego_per_nome(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "export"
            source.mkdir()
            assets = source / "allegati rinominati"
            assets.mkdir()
            destination = root / "sito"
            destination.mkdir()

            identifier = "51cb1311-4e77-409e-90fd-0dde04a9ae85"
            report = "3x8e4tPkAD25c9LjYr67gF"
            image = "00000000a94882bbf63e980997d3111e"
            partial = "00000000b62849cabcd532814e89175e"
            audio = "00000000adb6497a8e639a4701c13606"
            raw = {
                "conversation_id": identifier,
                "title": "Titolo aggiornato",
                "current_node": "m",
                "mapping": {"m": {"message": {
                    "author": {"role": "user"},
                    "content": {"parts": [{
                        "content_type": "image_asset_pointer",
                        "asset_pointer": f"file-service://file_{image}?user=123",
                    }, {
                        "content_type": "image_asset_pointer",
                        "asset_pointer": "sediment://file_senza_nome",
                    }]},
                    "metadata": {"attachments": [
                        {"id": f"file-{report}", "name": "Budget.pdf"},
                        {"id": f"file_{partial}", "name": "Sottotitoli.txt"},
                        {"file_id": "file_fallback", "filename": "Relazione consolidata.txt"},
                        {"id": "file_missing", "name": "Rapporto 2025.pdf"},
                        {"id": "file_abc", "name": "Versione corta.pdf"},
                    ]},
                }}, "alternativo": {"message": {
                    "author": {"role": "assistant"},
                    "content": {"parts": ["Testo del ramo alternativo"]},
                    "metadata": {"attachments": [
                        {"id": f"file_{audio}", "name": "Lettura", "mime_type": "audio/wav"},
                        {"id": "file_abcd"},
                    ]},
                }}},
            }
            (source / "chatgpt_business_export_COMPLETO_2026-11-23 (1).json").write_text(
                json.dumps([{"id": identifier, "title": raw["title"], "raw_data": raw}])
            )
            (source / f"0001_Titolo_precedente_{identifier} (1).json").write_text(json.dumps(raw))
            (source / "0001_Titolo_precedente_Budget.pdf").write_bytes(b"%PDF-1.7\nfile omonimo")
            (source / "0001_Titolo_precedente_Relazzione_consolidata.txt").write_text("<script>pericolo</script>")
            (source / "0001_Titolo_precedente_Rapporto_2024.pdf").write_bytes(b"%PDF-1.7\nanno diverso")
            (source / "0001_Titolo_precedente_generated_image.png").write_bytes(b"Immagine senza ID nel nome")
            (assets / f"0098_vecchionomefile_{report}rinominato (1).bin").write_bytes(b"%PDF-1.7\nID giusto")
            png = b"\x89PNG\r\n\x1a\nContenuto di prova"
            (assets / f"copia{image}finale.download").write_bytes(png)
            (assets / f"file_{partial[:-5]}").write_bytes(b"ID troncato")
            (assets / f"audioFILE-{audio}recuperato").write_bytes(b"\x00audio\xff")
            (assets / "nomefile-abcdversione").write_bytes(b"\x00File sconosciuto\xff")

            site = destination / asyncio.run(genera_da_cartelle(source, destination, False))
            manifest = (site / "chat/1-file.js").read_text()
            data = json.loads(manifest.removeprefix("window.registraFile(").removesuffix(");\n"))
            links = data["links"]

            self.assertEqual(set(links), {
                f"file_{report}", f"file_{image}", f"file_{partial}", "file_fallback", "file_senza_nome",
                f"file_{audio}", "file_abcd",
            })
            self.assertEqual((site / links[f"file_{report}"]["url"]).read_bytes(), b"%PDF-1.7\nID giusto")
            self.assertEqual(links[f"file_{report}"]["name"], "Budget.pdf")
            self.assertTrue(links[f"file_{report}"]["pdf"])
            self.assertEqual(links[f"file_{image}"]["name"], "copiafinale.png")
            self.assertEqual((site / links[f"file_{image}"]["url"]).read_bytes(), png)
            self.assertEqual((site / links[f"file_{partial}"]["url"]).read_bytes(), b"ID troncato")
            self.assertEqual(links[f"file_{partial}"]["name"], "Sottotitoli.txt")
            self.assertEqual(links[f"file_{audio}"]["name"], "Lettura.wav")
            self.assertEqual(links["file_abcd"]["name"], "nomeversione.bin")
            self.assertEqual(links["file_senza_nome"]["name"], "generated image.png")
            self.assertTrue((site / unquote(links["file_fallback"]["url"])).is_file())
            self.assertNotIn("Testo del ramo alternativo", (site / "chat/1.js").read_text())
            self.assertIn("Budget (2).pdf", [file["name"] for file in data["all"]])
            text_preview = (site / links["file_fallback"]["preview"]).read_text()
            self.assertIn("&lt;script&gt;pericolo&lt;/script&gt;", text_preview)
            self.assertNotIn("<script>", text_preview)
            self.assertNotIn(identifier, " ".join(file["name"] for file in data["all"]))


if __name__ == "__main__":
    unittest.main()
