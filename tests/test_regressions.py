import gzip
import io
import json
import logging
import subprocess
import tempfile
import unittest
import zipfile
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

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


def read_jsonl_gz(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


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


def sample_doc(
    doc_id: str,
    file_key: str,
    file_path: str,
    file_name: str | None = None,
    *,
    category: str = "Other",
    extraction_status: str = "ok",
    review_flags: str = "",
) -> docatlas.DocRecord:
    return docatlas.DocRecord(
        doc_id=doc_id,
        file_key=file_key,
        file_name=file_name or Path(file_path).name,
        file_path=file_path,
        source_path=file_path,
        file_ext=Path(file_path).suffix.lower() or ".xlsx",
        category=category,
        tags=["tag1", "tag2"],
        short_summary="Test summary",
        long_summary="Longer test summary for workbook output.",
        word_count=25,
        char_count=180,
        extraction_status=extraction_status,
        review_flags=review_flags,
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
                "Nanodrop": "/Life_Sciences/Life_Science_Applications/TestApp/Nanodrop",
                "Protein expression": "/Life_Sciences/Life_Science_Applications/TestApp/Protein_Expression",
                "Water": "/Life_Sciences/Life_Science_Applications/TestApp/Water",
                "Custom DNA Oligos": "/Life_Sciences/Life_Science_Applications/TestApp/Custom_DNA_Oligos",
                "Dynabeads": "/Life_Sciences/Life_Science_Applications/TestApp/Dynabeads",
            }
        }

    def prepare_logging(self, out_dir: Path) -> None:
        docatlas.setup_logging(out_dir)
        self.addCleanup(logging.shutdown)

    def test_write_unsupported_cleanup_workbook_formats_and_sorts(self) -> None:
        output_dir = self.make_tempdir()
        items = [
            docatlas.UnsupportedFileRecord("b.msg", "team/inbox/b.msg", ".msg", "file"),
            docatlas.UnsupportedFileRecord("a.url", "team/inbox/a.url", ".url", "file"),
            docatlas.UnsupportedFileRecord("inside.bin", "bundle.zip!/nested/inside.bin", ".bin", "zip_member"),
            docatlas.UnsupportedFileRecord("broken.zip", "broken.zip", ".zip", "invalid_zip"),
            docatlas.UnsupportedFileRecord("c.url", "other/c.url", ".url", "file"),
        ]

        workbook_path = docatlas.write_unsupported_cleanup_workbook(output_dir, items)

        self.assertTrue(workbook_path.exists())
        wb = load_workbook(workbook_path)
        self.assertEqual(wb.sheetnames, ["cleanup_queue", "cleanup_legend"])

        queue_ws = wb["cleanup_queue"]
        headers = [queue_ws.cell(row=1, column=c).value for c in range(1, queue_ws.max_column + 1)]
        self.assertEqual(
            headers,
            [
                "FileType",
                "FileName",
                "FilePath",
                "SourceKind",
                "SourceFolder",
                "RecommendedAction",
                "DeleteCandidate",
                "Decision",
                "DecisionNotes",
                "ReviewedBy",
                "FinalDisposition",
            ],
        )
        self.assertEqual(queue_ws.freeze_panes, "A2")
        self.assertEqual(queue_ws.auto_filter.ref, "A1:K6")
        self.assertEqual(len(queue_ws.data_validations.dataValidation), 1)
        self.assertTrue(len(queue_ws.conditional_formatting) >= 1)
        self.assertEqual(queue_ws["E2"].value, "team/inbox")
        self.assertEqual(queue_ws["E3"].value, "team/inbox")
        self.assertEqual(queue_ws["F2"].value, "Review")
        self.assertFalse(bool(queue_ws["G2"].value))
        legend_ws = wb["cleanup_legend"]
        self.assertEqual(legend_ws["A2"].value, "Keep")
        self.assertEqual(legend_ws["A5"].value, "Needs Follow-up")

    def test_process_file_supports_xls_via_conversion(self) -> None:
        root = self.make_tempdir()
        xls_path = root / "legacy.xls"
        xls_path.write_bytes(b"fake xls placeholder")
        converted_path = root / "converted.xlsx"
        write_xlsx(converted_path, " ".join(["legacy xls content"] * 20))

        with patch("docatlas.convert_xls_to_xlsx", return_value=converted_path):
            text, articles, status = docatlas.process_file(
                xls_path,
                ocrmypdf_enabled=False,
                articles_enabled=False,
                source_label="legacy.xls",
            )

        self.assertIn("legacy xls content", text)
        self.assertEqual(articles, [])
        self.assertEqual(status, "ok")

    def test_detect_duplicates_does_not_use_embedding_similarity_for_exact(self) -> None:
        duplicate_of, duplicate_score, duplicate_group = docatlas.detect_duplicates(
            [
                ("DOC-1", "hash-a", docatlas.np.array([1.0, 0.0], dtype=docatlas.np.float32)),
                ("DOC-2", "hash-b", docatlas.np.array([0.999, 0.001], dtype=docatlas.np.float32)),
            ],
            docatlas.DUPLICATE_THRESHOLD,
        )
        self.assertEqual(duplicate_of, {})
        self.assertEqual(duplicate_score, {})
        self.assertEqual(duplicate_group, {})

    def test_run_pipeline_failed_spreadsheets_do_not_collapse_into_exact_duplicates(self) -> None:
        root = self.make_tempdir()
        input_dir = root / "input"
        output_dir = root / "output"
        input_dir.mkdir(parents=True, exist_ok=True)
        (input_dir / "broken_a.xlsx").write_bytes(b"not an xlsx a")
        (input_dir / "broken_b.xlsx").write_bytes(b"not an xlsx b")

        docatlas.run_pipeline(
            input_dir=input_dir,
            output_dir=output_dir,
            categories=["Other"],
            cfg=dummy_cfg(),
            dry_run=True,
            use_resume=False,
            ocrmypdf_enabled=False,
            app_name="TestApp",
            embeddings_source=docatlas.EMBEDDINGS_SOURCE_NONE,
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            no_move=True,
            articles_enabled=False,
        )
        self.addCleanup(logging.shutdown)

        docs_df = pd.read_excel(output_dir / "TestApp__docatlas_summaries.xlsx", sheet_name="Documents")
        self.assertEqual(len(docs_df), 2)
        self.assertTrue((docs_df["ExtractionStatus"] == "no_text").all())
        self.assertTrue((docs_df["DuplicateOf"].fillna("") == "").all())
        self.assertTrue((docs_df["DupScore"].fillna(0.0) == 0.0).all())

    def test_weak_near_duplicate_edges_require_structural_signal(self) -> None:
        docs = [
            sample_doc("DOC-1", "k1", "nanodrop/enablement_final.pptx", category="CatA"),
            sample_doc("DOC-2", "k2", "xenon/introduction_overview.pptx", category="CatA"),
            sample_doc("DOC-3", "k3", "crispr/product_alpha_overview.pdf", category="CatB"),
            sample_doc("DOC-4", "k4", "crispr/product_alpha_update.docx", category="CatB"),
        ]
        score = 0.80
        vec_a = docatlas.np.array([1.0, 0.0], dtype=docatlas.np.float32)
        vec_b = docatlas.np.array([score, (1 - score**2) ** 0.5], dtype=docatlas.np.float32)
        doc_embeddings = {
            "DOC-1": vec_a,
            "DOC-2": vec_b,
            "DOC-3": vec_a,
            "DOC-4": vec_b,
        }

        near_of, _near_score, _near_group, _near_adj, near_edges = docatlas.detect_near_duplicates_docs(docs, doc_embeddings)

        self.assertNotIn(("DOC-1", "DOC-2"), near_edges)
        self.assertIn(("DOC-3", "DOC-4"), near_edges)
        self.assertEqual(near_of.get("DOC-1", ""), "")
        self.assertIn(near_of.get("DOC-3", ""), {"DOC-4"})

    def test_article_type_prefers_manuals_over_troubleshooting_content(self) -> None:
        label = docatlas.classify_article_type_by_content(
            "superscript_user_guide.pdf",
            "SuperScript User Guide",
            "Guide for setup and operation.",
            "Includes a troubleshooting section and error examples.",
            "This user guide explains setup, operation, troubleshooting, and maintenance.",
        )
        self.assertEqual(label, "Manuals and Guides")

        label = docatlas.classify_article_type_by_content(
            "qpcr_error_resolution.pdf",
            "qPCR Error Resolution",
            "How to resolve common issues.",
            "Focuses on failure causes and corrective actions.",
            "Troubleshooting errors, failures, and issue resolution steps.",
        )
        self.assertEqual(label, "Troubleshooting")

    def test_write_excels_excludes_unreadable_docs_from_import(self) -> None:
        out_dir = self.make_tempdir()
        self.prepare_logging(out_dir)
        good_doc = sample_doc("20260305111752-DOC-00001", "key-1", "sub/a.xlsx", category="Other")
        unreadable_doc = sample_doc(
            "20260305111752-DOC-00002",
            "key-2",
            "sub/b.xlsx",
            category=docatlas.UNREADABLE_CATEGORY,
            extraction_status="no_text",
            review_flags="low_text,short_text",
        )

        docatlas.write_excels(
            out_dir=out_dir,
            docs=[good_doc, unreadable_doc],
            articles=[],
            full_text_rows=[
                {"doc_id": good_doc.doc_id, "full_text": "good body text"},
                {"doc_id": unreadable_doc.doc_id, "full_text": "bad body text"},
            ],
            app_name="TestApp",
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=False,
        )

        import_df = pd.read_excel(out_dir / "TestApp__docatlas_import.xlsx", sheet_name="import")
        self.assertEqual(len(import_df), 1)
        self.assertEqual(str(import_df.loc[0, "Title"]), good_doc.short_summary)

    def test_large_doc_summary_guard_uses_local_fallback(self) -> None:
        huge_text = ("important assay content " * 20000).strip()
        with patch("docatlas.summarize_document", side_effect=AssertionError("chat path should not run")):
            summary, flag = docatlas.summarize_document_safe(
                dummy_cfg(),
                huge_text,
                ["Other"],
                "huge.xlsx",
                "root/huge.xlsx",
            )

        self.assertEqual(flag, "summary_truncated_large_doc")
        self.assertTrue(summary["long_summary"])
        self.assertEqual(summary["category"], "Other")

    def test_infer_category_uses_path_hints_for_nanodrop(self) -> None:
        category = docatlas._infer_category_from_text(
            "Training overview for a microvolume UV-Vis spectrophotometer and dealer enablement.",
            ["Nanodrop", "Other"],
            file_name="NanoDrop8_ProductAwareness_21SEP2021.pptx",
            file_path="Nanodrop/CONFIDENTIAL/NanoDrop Eight/NanoDrop8_ProductAwareness_21SEP2021.pptx",
        )
        self.assertEqual(category, "Nanodrop")

    def test_infer_category_uses_path_hints_for_protein_expression(self) -> None:
        category = docatlas._infer_category_from_text(
            "Training quiz covering promoters, cloning methods, and algal engineering.",
            ["Protein expression", "Other"],
            file_name="Algal Engineering_Prod_Quiz.docx",
            file_path="Protein expression/CONFIDENTIAL/Algae/Algal Engineering_Prod_Quiz.docx",
        )
        self.assertEqual(category, "Protein expression")

    def test_infer_category_prefers_custom_dna_oligos_over_water_for_oligo_coa(self) -> None:
        category = docatlas._infer_category_from_text(
            (
                "Certificate of analysis for custom DNA primers with sequences, "
                "well positions, extinction coefficient, GC content, and reconstitution guidance."
            ),
            ["Custom DNA Oligos", "Water", "Other"],
            file_name="18789122_e_plate.xls",
            file_path="PCR/Primers/Oligo Files/Randall-Primers/CoAs/18789122_e_plate.xls",
        )
        self.assertEqual(category, "Custom DNA Oligos")

    def test_infer_category_requires_explicit_water_signal(self) -> None:
        category = docatlas._infer_category_from_text(
            (
                "MSDS for Dynabeads anti-E.coli suspension with sodium phosphate buffer, "
                "safe handling guidance, and references to water samples."
            ),
            ["Dynabeads", "Water", "Other"],
            file_name="71003_MTR-NAIV_EN.pdf",
            file_path="Dynabeads/Dynabeads/CONFIDENTIAL/Bacterial Microbilogy/Documentation/MSDSs/71003_MTR-NAIV_EN.pdf",
        )
        self.assertEqual(category, "Dynabeads")

    def test_molecular_biology_config_includes_nanodrop_and_protein_expression(self) -> None:
        with open("applications.json", encoding="utf-8") as fh:
            app_config = json.load(fh)["applications"]
        with open("category_path_map.json", encoding="utf-8") as fh:
            path_map = json.load(fh)

        for app_name in ["Molecular Biology", "molbio"]:
            self.assertIn("Nanodrop", app_config[app_name])
            self.assertIn("Protein expression", app_config[app_name])
            self.assertIn("Nanodrop", path_map[app_name])
            self.assertIn("Protein expression", path_map[app_name])

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
        archive_path = out_dir / "TestApp__docatlas_full_text.jsonl.gz"
        self.assertTrue(archive_path.exists())
        self.assertFalse((out_dir / "TestApp__docatlas_full_text.xlsx").exists())

    def test_write_excels_writes_full_text_archive_by_default_and_dedupes_append(self) -> None:
        out_dir = self.make_tempdir()
        self.prepare_logging(out_dir)
        first_doc = sample_doc("20260305111752-DOC-00001", "key-1", "sub/a.xlsx")
        second_doc = sample_doc("20260305111752-DOC-00002", "key-2", "sub/b.xlsx")

        docatlas.write_excels(
            out_dir=out_dir,
            docs=[first_doc],
            articles=[],
            full_text_rows=[
                {
                    "doc_id": first_doc.doc_id,
                    "file_key": first_doc.file_key,
                    "file_name": first_doc.file_name,
                    "file_path": first_doc.file_path,
                    "category": first_doc.category,
                    "short_summary": first_doc.short_summary,
                    "long_summary": first_doc.long_summary,
                    "tags": ", ".join(first_doc.tags),
                    "word_count": first_doc.word_count,
                    "char_count": first_doc.char_count,
                    "extraction_status": first_doc.extraction_status,
                    "review_flags": first_doc.review_flags,
                    "moved_to": first_doc.moved_to,
                    "full_text": "first body text",
                }
            ],
            app_name="TestApp",
            append_excel=False,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=False,
        )

        archive_path = out_dir / "TestApp__docatlas_full_text.jsonl.gz"
        records = read_jsonl_gz(archive_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["file_key"], "key-1")
        self.assertEqual(records[0]["full_text"], "first body text")

        docatlas.write_excels(
            out_dir=out_dir,
            docs=[first_doc, second_doc],
            articles=[],
            full_text_rows=[
                {
                    "doc_id": first_doc.doc_id,
                    "file_key": first_doc.file_key,
                    "file_name": first_doc.file_name,
                    "file_path": first_doc.file_path,
                    "category": first_doc.category,
                    "short_summary": first_doc.short_summary,
                    "long_summary": first_doc.long_summary,
                    "tags": ", ".join(first_doc.tags),
                    "word_count": first_doc.word_count,
                    "char_count": first_doc.char_count,
                    "extraction_status": first_doc.extraction_status,
                    "review_flags": first_doc.review_flags,
                    "moved_to": first_doc.moved_to,
                    "full_text": "first body text again",
                },
                {
                    "doc_id": second_doc.doc_id,
                    "file_key": second_doc.file_key,
                    "file_name": second_doc.file_name,
                    "file_path": second_doc.file_path,
                    "category": second_doc.category,
                    "short_summary": second_doc.short_summary,
                    "long_summary": second_doc.long_summary,
                    "tags": ", ".join(second_doc.tags),
                    "word_count": second_doc.word_count,
                    "char_count": second_doc.char_count,
                    "extraction_status": second_doc.extraction_status,
                    "review_flags": second_doc.review_flags,
                    "moved_to": second_doc.moved_to,
                    "full_text": "second body text",
                },
            ],
            app_name="TestApp",
            append_excel=True,
            category_path_map=self.category_path_map(),
            include_full_text_output=False,
            articles_enabled=False,
        )

        records = read_jsonl_gz(archive_path)
        self.assertEqual([record["file_key"] for record in records], ["key-1", "key-2"])
        self.assertEqual(records[1]["full_text"], "second body text")

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
        self.assertTrue((out_dir / "TestApp__docatlas_full_text.jsonl.gz").exists())
        self.assertFalse((out_dir / "TestApp__docatlas_full_text.xlsx").exists())

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
        self.assertTrue((output_dir / "TestApp__docatlas_full_text.jsonl.gz").exists())
        self.assertFalse((output_dir / "TestApp__docatlas_full_text.xlsx").exists())
        self.assertFalse((output_dir / "unsupported_cleanup.xlsx").exists())
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
        self.assertTrue((output_dir / "TestApp__docatlas_full_text.jsonl.gz").exists())
        self.assertFalse((output_dir / "TestApp__docatlas_full_text.xlsx").exists())
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
        cleanup_wb = load_workbook(output_dir / "unsupported_cleanup.xlsx")
        self.assertIn("total_unsupported_files: 3", summary_text)
        self.assertIn("- .mp4: 1", summary_text)
        self.assertIn("- .url: 1", summary_text)
        self.assertIn("- .zip: 1", summary_text)
        self.assertIn("Detailed List:", unsupported_text)
        self.assertIn(".url | link.url | link.url", unsupported_text)
        self.assertIn(".zip | broken.zip | broken.zip", unsupported_text)
        self.assertEqual(cleanup_wb.sheetnames, ["cleanup_queue", "cleanup_legend"])
        queue_df = pd.read_excel(output_dir / "unsupported_cleanup.xlsx", sheet_name="cleanup_queue")
        self.assertEqual(len(queue_df), 3)
        self.assertEqual(queue_df["RecommendedAction"].tolist(), ["Review", "Review", "Review"])
        self.assertTrue((queue_df["DeleteCandidate"] == False).all())

    def test_run_pipeline_writes_unsupported_cleanup_workbook_for_mixed_input(self) -> None:
        root = self.make_tempdir()
        input_dir = root / "input"
        output_dir = root / "output"
        write_xlsx(
            input_dir / "sub" / "direct.xlsx",
            " ".join(["mixed pipeline content application note guide"] * 20),
        )
        (input_dir / "sub" / "shortcut.url").write_text("[InternetShortcut]\nURL=https://example.com\n", encoding="utf-8")
        with zipfile.ZipFile(input_dir / "bundle.zip", "w") as zf:
            zf.writestr(
                "nested/inside.xlsx",
                make_workbook_bytes(" ".join(["mixed zipped content"] * 25)),
            )
            zf.writestr("nested/inside.url", "[InternetShortcut]\nURL=https://example.com\n")

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

        self.assertTrue((output_dir / "TestApp__docatlas_summaries.xlsx").exists())
        cleanup_df = pd.read_excel(output_dir / "unsupported_cleanup.xlsx", sheet_name="cleanup_queue")
        self.assertEqual(len(cleanup_df), 2)
        self.assertEqual(set(cleanup_df["SourceKind"].astype(str)), {"file", "zip_member"})
        self.assertIn("sub", set(cleanup_df["SourceFolder"].astype(str)))
        self.assertIn("bundle.zip!/nested", set(cleanup_df["SourceFolder"].astype(str)))

    def test_resolve_cli_interactive_inputs_prompts_for_missing_values(self) -> None:
        root = self.make_tempdir()
        input_dir = root / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir = root / "output"

        args = Namespace(
            input=str(input_dir),
            output=None,
            categories=None,
            app="TestApp",
            charter_mode=False,
            signal_scan=False,
            no_move=False,
            no_ocrmypdf=False,
            embeddings_source=None,
            overwrite_excel=False,
            articles=False,
            workers=1,
        )

        with patch("sys.stdin.isatty", return_value=True):
            with patch(
                "builtins.input",
                side_effect=[
                    str(output_dir),
                    "",
                    "",
                    "",
                    "",
                    "y",
                    "2",
                ],
            ):
                result = docatlas.resolve_cli_interactive_inputs(args, [], {"TestApp": ["Other"]})

        (
            resolved_input,
            resolved_output,
            categories,
            app_name,
            ocrmypdf_enabled,
            embeddings_source,
            append_excel,
            no_move,
            articles_enabled,
            workers,
        ) = result
        self.assertEqual(resolved_input, input_dir)
        self.assertEqual(resolved_output, output_dir)
        self.assertEqual(categories, ["Other"])
        self.assertEqual(app_name, "TestApp")
        self.assertTrue(ocrmypdf_enabled)
        self.assertEqual(embeddings_source, "full_text")
        self.assertTrue(append_excel)
        self.assertTrue(no_move)
        self.assertTrue(articles_enabled)
        self.assertEqual(workers, 2)

    def test_build_app_folder_structure_script_creates_expected_layout(self) -> None:
        root = self.make_tempdir()
        base_dir = root / "DocAtlas"
        config_path = root / "applications.json"
        config_path.write_text(
            json.dumps({"applications": {"Molecular Biology": ["Other"], "qPCR": ["Other"]}}),
            encoding="utf-8",
        )

        result = subprocess.run(
            [
                "python",
                "build_app_folder_structure.py",
                "--base",
                str(base_dir),
                "--config",
                str(config_path),
            ],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=True,
        )

        self.assertTrue((base_dir / "input" / "molecular_biology").exists())
        self.assertTrue((base_dir / "output" / "molecular_biology" / "charter").exists())
        self.assertTrue((base_dir / "output" / "molecular_biology" / "atlas").exists())
        self.assertTrue((base_dir / "input" / "qpcr").exists())
        self.assertTrue((base_dir / "archive" / "zips").exists())
        self.assertIn("Molecular Biology -> molecular_biology", result.stdout)
        self.assertIn("qPCR -> qpcr", result.stdout)


if __name__ == "__main__":
    unittest.main()
