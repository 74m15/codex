import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from genera_archivio import genera_da_cartelle


class AttachmentTests(unittest.TestCase):
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
            (assets / f"0098_file_vecchio_nome_file_{report}_rinominato (1).pdf").write_bytes(b"%PDF-1.7\nID giusto")
            (assets / f"file-{image}.png").write_bytes(b"PNG di prova")
            (assets / f"file_{partial[:-5]}").write_bytes(b"ID troncato")

            site = destination / asyncio.run(genera_da_cartelle(source, destination, False))
            manifest = (site / "chat/1-file.js").read_text()
            data = json.loads(manifest.removeprefix("window.registraFile(").removesuffix(");\n"))
            links = data["links"]

            self.assertEqual(set(links), {
                f"file_{report}", f"file_{image}", f"file_{partial}", "file_fallback", "file_senza_nome",
            })
            self.assertEqual((site / links[f"file_{report}"]["url"]).read_bytes(), b"%PDF-1.7\nID giusto")
            self.assertTrue(links[f"file_{report}"]["pdf"])
            self.assertEqual((site / links[f"file_{image}"]["url"]).read_bytes(), b"PNG di prova")
            self.assertEqual((site / links[f"file_{partial}"]["url"]).read_bytes(), b"ID troncato")
            self.assertTrue(links[f"file_{partial}"]["url"].endswith(".txt"))
            self.assertEqual(links["file_senza_nome"]["name"], "generated_image.png")
            text_preview = (site / links["file_fallback"]["preview"]).read_text()
            self.assertIn("&lt;script&gt;pericolo&lt;/script&gt;", text_preview)
            self.assertNotIn("<script>", text_preview)
            self.assertNotIn(identifier, " ".join(file["name"] for file in data["all"]))


if __name__ == "__main__":
    unittest.main()
