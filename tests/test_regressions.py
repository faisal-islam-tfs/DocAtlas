import io
import logging
import tempfile
import unittest
import zipfile
from pathlib import Path

import pandas as pd
from openpyxl import Workbook, load_workbook

import docatlas


def make_workbook_bytes(text: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    ws["A1"] = "Title"
    ws["A2"] = "DocAtlas"
    ws["B2"] = text
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def write_xlsx(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(make_workbook_bytes(text))


def dummy_cfg() -> docatlas.AzureConfig:
    return docatlas.AzureConfig(
        api_key="",
        chat_api_key="",
        embeddings_api_key="",
        api_version="",
        api_key_header="api-key",
        chat_base_url="",
        chat_path="",
        chat_deployment="",
        embeddings_base_url="",
        embeddings_path="",
        embeddings_deployment="",
        include_model_in_body=False,
    )


def sample_doc(doc_id: str, file_key: str, file_path: str, file_name: str | None = None) -> docatlas.DocRecord:
    return docatlas.DocRecord(
        doc_id=doc_id,
        file_key=file_key,
        file_name=file_name or Path(file_path).name,
        file_path=file_path,
        source_path=file_path,
        file_ext=Path(file_path).suffix.lower() or ".xlsx",
        category="Other",
        tags=["tag1", "tag2"],
        short_summary="Test summary",
        long_summary="Longer test summary for workbook output.",
        word_count=25,
        char_count=180,
        extraction_status="ok",
        review_flags="",
        duplicate_of="",
        duplicate_score=None,
        duplicate_group_id="",
        near_duplicate_of="",
        near_duplicate_score=None,
        near_duplicate_group_id="",
        review_group_id="",
        duplicate_relation_type="",
        moved_to="",
    )


def sample_article(doc_id: str, file_key: str, file_path: str) -> docatlas.ArticleRecord:
    return docatlas.ArticleRecord(
        doc_id=doc_id,
        file_key=file_key,
        file_name=Path(file_path).name,
        file_path=file_path,
        article_index=1,
        article_title="Article title",
        article_summary="Article summary",
        duplicate_of="",
        duplicate_score=None,
        duplicate_group_id="",
    )


class DocAtlasRegressionTests(unittest.TestCase):
    def make_tempdir(self) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        return Path(temp_dir.name)

    def category_path_map(self) -> dict:
        return {
            "TestApp": {
                "Other": "/Life_Sciences/Life_Science_Applications/TestApp/Other",
            }
        }

    def prepare_logging(self, out_dir: Path) -> None:
        docatlas.setup_logging(out_dir)
        self.addCleanup(logging.shutdown)

    def test_list_files_discovers_zip_members_with_logical_paths(self) -> None:
        input_dir = self.make_tempdir()
        write_xlsx(
            input_dir / "sub" / "direct.xlsx",
            " ".join(["direct workbook content"] * 20),
        )
        (input_dir / "sub" / "skip.url").write_text("[InternetShortcut]\nURL=https://example.com\n", encoding="utf-8")
        bundle_path = input_dir / "bundle.zip"
        with zipfile.ZipFile(bundle_path, "w") as zf:
            zf.writestr(
                "nested/inside.xlsx",
                make_workbook_bytes(" ".join(["zipped workbook content"] * 20)),
            )
            zf.writestr("nested/clip.mp4", b"not really a movie")

        files, unsupported, staged = docatlas.list_files(input_dir)
        try:
            display_paths = [item.display_path for item in files]
            self.assertEqual(
                display_paths,
                ["bundle.zip!/nested/inside.xlsx", "sub/direct.xlsx"],
            )
            unsupported_map = {(item.file_type, item.file_path, item.source_kind) for item in unsupported}
            self.assertIn((".url", "sub/skip.url", "file"), unsupported_map)
            self.assertIn((".mp4", "bundle.zip!/nested/clip.mp4", "zip_member"), unsupported_map)
        finally:
            if staged is not None:
                staged.cleanup()

    def test_list_files_records_invalid_zip_as_unsupported(self) -> None:
        input_dir = self.make_tempdir()
        (input_dir / "broken.zip").write_text("not a real zip", encoding="utf-8")

        files, unsupported, staged = docatlas.list_files(input_dir)
        try:
            self.assertEqual(files, [])
            self.assertEqual(len(unsupported), 1)
            self.assertEqual(unsupported[0].file_path, "broken.zip")
            self.assertEqual(unsupported[0].source_kind, "invalid_zip")
        finally:
            if staged is not None:
                staged.cleanup()

    def test_build_import_path_uses_article_type_slug(self) -> None:
        path = docatlas.build_import_path(
            "/Life_Sciences/Life_Science_Applications/Quantitative_PCR/Real-Time_PCR/TaqMan_Assays_and_Applications/TaqMan_Protein_Assay",
            "TaqMan Protein Assay",
            "Application and Product Notes",
        )
        self.assertEqual(
            path,
            "/Life_Sciences/Life_Science_Applications/Quantitative_PCR/Real-Time_PCR/TaqMan_Assays_and_Applications/TaqMan_Protein_Assay/TaqMan_Protein_Assay_Application_and_Product_Notes",
        )

    def test_write_excels_dedupes_append_across_absolute_and_relative_paths(self) -> None:
        out_dir = self.make_tempdir()
        self.prepare_logging(out_dir)
        peers_path = out_dir / "TestApp__docatlas_summaries.xlsx"
        with pd.ExcelWriter(peers_path, engine="openpyxl") as writer:
            pd.DataFrame(
                [{"Category": "Other", "FilePath": "C:/root/input/sub/a.xlsx", "FileName": "a.xlsx"}]
            ).to_excel(writer, index=False, sheet_name="Documents")
            pd.DataFrame(columns=["ReviewGroupID", "Category", "FilePath", "FileName"]).to_excel(
                writer,
                index=False,
                sheet_name="Duplicates",
            )

        doc = sample_doc("20260305111752-DOC-00001", "new-key", "sub/a.xlsx")
        docatlas.write_excels(
            out_dir=out_dir,
            docs=[doc],
            articles=[],
            full_text_rows=[{"doc_id": doc.doc_id, "full_text": "body text"}],
            app_name="TestApp",
            append_excel=True,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=False,
        )

        docs_df = pd.read_excel(peers_path, sheet_name="Documents")
        self.assertEqual(len(docs_df), 1)
        self.assertEqual(docs_df.loc[0, "FilePath"], "C:/root/input/sub/a.xlsx")

    def test_write_excels_omits_articles_sheet_when_disabled(self) -> None:
        out_dir = self.make_tempdir()
        self.prepare_logging(out_dir)
        doc = sample_doc("20260305111752-DOC-00001", "key-1", "sub/a.xlsx")
        docatlas.write_excels(
            out_dir=out_dir,
            docs=[doc],
            articles=[sample_article(doc.doc_id, doc.file_key, doc.file_path)],
            full_text_rows=[{"doc_id": doc.doc_id, "full_text": "body text"}],
            app_name="TestApp",
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=False,
        )

        wb = load_workbook(out_dir / "TestApp__docatlas_summaries.xlsx")
        self.assertEqual(wb.sheetnames, ["Documents", "Duplicates"])

    def test_write_excels_preserves_existing_articles_sheet_on_append(self) -> None:
        out_dir = self.make_tempdir()
        self.prepare_logging(out_dir)
        first_doc = sample_doc("20260305111752-DOC-00001", "key-1", "sub/a.xlsx")
        first_article = sample_article(first_doc.doc_id, first_doc.file_key, first_doc.file_path)
        docatlas.write_excels(
            out_dir=out_dir,
            docs=[first_doc],
            articles=[first_article],
            full_text_rows=[{"doc_id": first_doc.doc_id, "full_text": "body text"}],
            app_name="TestApp",
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=True,
        )

        second_doc = sample_doc("20260305111752-DOC-00002", "key-2", "sub/b.xlsx")
        docatlas.write_excels(
            out_dir=out_dir,
            docs=[second_doc],
            articles=[],
            full_text_rows=[{"doc_id": second_doc.doc_id, "full_text": "body text"}],
            app_name="TestApp",
            append_excel=True,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=False,
        )

        peers_path = out_dir / "TestApp__docatlas_summaries.xlsx"
        wb = load_workbook(peers_path)
        self.assertIn("Articles", wb.sheetnames)
        articles_df = pd.read_excel(peers_path, sheet_name="Articles")
        self.assertEqual(len(articles_df), 1)
        self.assertEqual(str(articles_df.loc[0, "ParentDocID"]), first_doc.doc_id)

    def test_run_pipeline_serial_uses_relative_and_zip_logical_paths(self) -> None:
        root = self.make_tempdir()
        input_dir = root / "input"
        output_dir = root / "output"
        write_xlsx(
            input_dir / "sub" / "direct.xlsx",
            " ".join(["serial pipeline content application note guide"] * 20),
        )
        with zipfile.ZipFile(input_dir / "bundle.zip", "w") as zf:
            zf.writestr(
                "nested/inside.xlsx",
                make_workbook_bytes(" ".join(["serial zipped content"] * 25)),
            )

        docatlas.run_pipeline(
            input_dir=input_dir,
            output_dir=output_dir,
            categories=["Other"],
            cfg=dummy_cfg(),
            dry_run=True,
            use_resume=False,
            ocrmypdf_enabled=False,
            app_name="TestApp",
            embeddings_source="document",
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            no_move=True,
            articles_enabled=False,
        )
        self.addCleanup(logging.shutdown)

        peers_path = output_dir / "TestApp__docatlas_summaries.xlsx"
        wb = load_workbook(peers_path)
        self.assertEqual(wb.sheetnames, ["Documents", "Duplicates"])
        docs_df = pd.read_excel(peers_path, sheet_name="Documents")
        self.assertEqual(
            sorted(docs_df["FilePath"].astype(str).tolist()),
            ["bundle.zip!/nested/inside.xlsx", "sub/direct.xlsx"],
        )
        summary_text = (output_dir / "summary_report.txt").read_text(encoding="utf-8")
        unsupported_text = (output_dir / "unsupported_files_report.txt").read_text(encoding="utf-8")
        self.assertIn("total_unsupported_files: 0", summary_text)
        self.assertIn("Total unsupported files: 0", unsupported_text)

    def test_run_pipeline_parallel_writes_no_articles_sheet(self) -> None:
        root = self.make_tempdir()
        input_dir = root / "input"
        output_dir = root / "output"
        write_xlsx(
            input_dir / "a" / "one.xlsx",
            " ".join(["parallel pipeline content troubleshooting"] * 20),
        )
        with zipfile.ZipFile(input_dir / "bundle.zip", "w") as zf:
            zf.writestr(
                "nested/two.xlsx",
                make_workbook_bytes(" ".join(["parallel zipped content"] * 25)),
            )

        docatlas.run_pipeline_parallel(
            input_dir=input_dir,
            output_dir=output_dir,
            categories=["Other"],
            cfg=dummy_cfg(),
            dry_run=True,
            use_resume=False,
            ocrmypdf_enabled=False,
            app_name="TestApp",
            embeddings_source="document",
            append_excel=False,
            workers=2,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            no_move=True,
            articles_enabled=False,
        )
        self.addCleanup(logging.shutdown)

        peers_path = output_dir / "TestApp__docatlas_summaries.xlsx"
        wb = load_workbook(peers_path)
        self.assertEqual(wb.sheetnames, ["Documents", "Duplicates"])
        docs_df = pd.read_excel(peers_path, sheet_name="Documents")
        self.assertEqual(
            sorted(docs_df["FilePath"].astype(str).tolist()),
            ["a/one.xlsx", "bundle.zip!/nested/two.xlsx"],
        )

    def test_run_pipeline_writes_unsupported_reports_for_only_skipped_files(self) -> None:
        root = self.make_tempdir()
        input_dir = root / "input"
        output_dir = root / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        (input_dir / "link.url").write_text("[InternetShortcut]\nURL=https://example.com\n", encoding="utf-8")
        (input_dir / "movie.mp4").write_bytes(b"fake")
        (input_dir / "broken.zip").write_text("not a zip", encoding="utf-8")

        docatlas.run_pipeline(
            input_dir=input_dir,
            output_dir=output_dir,
            categories=["Other"],
            cfg=dummy_cfg(),
            dry_run=True,
            use_resume=False,
            ocrmypdf_enabled=False,
            app_name="TestApp",
            embeddings_source="document",
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            no_move=True,
            articles_enabled=False,
        )
        self.addCleanup(logging.shutdown)

        self.assertFalse((output_dir / "TestApp__docatlas_summaries.xlsx").exists())
        summary_text = (output_dir / "summary_report.txt").read_text(encoding="utf-8")
        unsupported_text = (output_dir / "unsupported_files_report.txt").read_text(encoding="utf-8")
        self.assertIn("total_unsupported_files: 3", summary_text)
        self.assertIn("- .mp4: 1", summary_text)
        self.assertIn("- .url: 1", summary_text)
        self.assertIn("- .zip: 1", summary_text)
        self.assertIn("Detailed List:", unsupported_text)
        self.assertIn(".url | link.url | link.url", unsupported_text)
        self.assertIn(".zip | broken.zip | broken.zip", unsupported_text)


if __name__ == "__main__":
    unittest.main()
