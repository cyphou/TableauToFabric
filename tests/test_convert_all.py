"""Tests for conversion.convert_all_tableau_objects — 0% → ~90% coverage."""

import json
import os
import shutil
import tempfile
import unittest

from conversion.convert_all_tableau_objects import (
    TableauToPowerBIConverter,
    CONVERSION_MODULES,
    main,
)


class TestTableauToPowerBIConverterInit(unittest.TestCase):
    """Test __init__ creates directories and initialises state."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_conv_")
        self.export_dir = os.path.join(self.tmpdir, "export")
        self.output_dir = os.path.join(self.tmpdir, "output")
        os.makedirs(self.export_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_output_and_logs_dirs(self):
        conv = TableauToPowerBIConverter(self.export_dir, self.output_dir)
        self.assertTrue(os.path.isdir(conv.output_dir))
        self.assertTrue(os.path.isdir(conv.logs_dir))

    def test_initial_stats(self):
        conv = TableauToPowerBIConverter(self.export_dir, self.output_dir)
        self.assertIn("start_time", conv.conversion_stats)
        self.assertEqual(conv.conversion_stats["errors"], [])
        self.assertEqual(conv.conversion_stats["warnings"], [])


class TestConvertObjectType(unittest.TestCase):
    """Test convert_object_type with various inputs."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_conv_")
        self.export_dir = os.path.join(self.tmpdir, "export")
        self.output_dir = os.path.join(self.tmpdir, "output")
        os.makedirs(self.export_dir, exist_ok=True)
        self.conv = TableauToPowerBIConverter(self.export_dir, self.output_dir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_missing_file_records_warning(self):
        # No JSON file on disk → warning
        self.conv.convert_object_type("worksheets", lambda x: x)
        self.assertEqual(self.conv.conversion_stats["objects_converted"]["worksheets"], 0)
        self.assertEqual(len(self.conv.conversion_stats["warnings"]), 1)
        self.assertIn("non trouvé", self.conv.conversion_stats["warnings"][0])

    def test_list_input_converted(self):
        data = [{"name": "ws1"}, {"name": "ws2"}]
        path = os.path.join(self.export_dir, "worksheets.json")
        with open(path, "w") as f:
            json.dump(data, f)
        self.conv.convert_object_type("worksheets", lambda x: {**x, "converted": True})
        self.assertEqual(self.conv.conversion_stats["objects_converted"]["worksheets"], 2)
        # Output file written
        out = os.path.join(self.output_dir, "worksheets_powerbi.json")
        self.assertTrue(os.path.isfile(out))
        with open(out) as f:
            result = json.load(f)
        self.assertTrue(all(o.get("converted") for o in result))

    def test_dict_input_wrapped_in_list(self):
        """When input is a single dict (not a list), it should be wrapped."""
        path = os.path.join(self.export_dir, "filters.json")
        with open(path, "w") as f:
            json.dump({"name": "single"}, f)
        self.conv.convert_object_type("filters", lambda x: x)
        self.assertEqual(self.conv.conversion_stats["objects_converted"]["filters"], 1)

    def test_converter_exception_per_object(self):
        """Errors on individual objects are captured but don't stop processing."""
        data = [{"name": "good"}, {"name": "bad"}, {"name": "good2"}]
        path = os.path.join(self.export_dir, "test_type.json")
        with open(path, "w") as f:
            json.dump(data, f)

        def flaky_converter(obj):
            if obj["name"] == "bad":
                raise ValueError("oops")
            return obj

        self.conv.convert_object_type("test_type", flaky_converter)
        # 2 converted (good + good2), 1 error
        self.assertEqual(self.conv.conversion_stats["objects_converted"]["test_type"], 2)
        self.assertEqual(len(self.conv.conversion_stats["errors"]), 1)
        self.assertIn("oops", self.conv.conversion_stats["errors"][0])

    def test_json_load_failure(self):
        """Invalid JSON triggers top-level exception handler."""
        path = os.path.join(self.export_dir, "bad.json")
        with open(path, "w") as f:
            f.write("NOT JSON")
        self.conv.convert_object_type("bad", lambda x: x)
        self.assertEqual(self.conv.conversion_stats["objects_converted"]["bad"], 0)
        self.assertEqual(len(self.conv.conversion_stats["errors"]), 1)


class TestConvertAll(unittest.TestCase):
    """Test convert_all orchestration."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_conv_all_")
        self.export_dir = os.path.join(self.tmpdir, "export")
        self.output_dir = os.path.join(self.tmpdir, "output")
        os.makedirs(self.export_dir, exist_ok=True)
        # Create stub JSON files for each module type
        for obj_type in CONVERSION_MODULES:
            path = os.path.join(self.export_dir, f"{obj_type}.json")
            with open(path, "w") as f:
                json.dump([], f)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_convert_all_creates_report_and_stats(self):
        conv = TableauToPowerBIConverter(self.export_dir, self.output_dir)
        conv.convert_all()
        stats_path = os.path.join(self.output_dir, "conversion_stats.json")
        self.assertTrue(os.path.isfile(stats_path))
        with open(stats_path) as f:
            stats = json.load(f)
        self.assertIn("total", stats)
        self.assertEqual(stats["total"], 0)


class TestGenerateConversionReport(unittest.TestCase):
    """Test generate_conversion_report produces valid JSON."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_report_")
        self.conv = TableauToPowerBIConverter(
            os.path.join(self.tmpdir, "e"), os.path.join(self.tmpdir, "o")
        )
        # Override logs_dir to use temp directory so we don't pollute shared dir
        self.conv.logs_dir = os.path.join(self.tmpdir, "logs")
        os.makedirs(self.conv.logs_dir, exist_ok=True)
        self.conv.conversion_stats["objects_converted"] = {"worksheets": 3}
        self.conv.conversion_stats["errors"] = ["err1"]
        self.conv.conversion_stats["warnings"] = ["w1", "w2"]

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_report_file_created(self):
        self.conv.generate_conversion_report()
        files = [f for f in os.listdir(self.conv.logs_dir) if f.startswith("conversion_report_")]
        self.assertEqual(len(files), 1)
        with open(os.path.join(self.conv.logs_dir, files[0])) as f:
            report = json.load(f)
        self.assertEqual(report["summary"]["total_objects"], 3)
        self.assertEqual(report["summary"]["error_count"], 1)
        self.assertEqual(report["summary"]["warning_count"], 2)


class TestSaveConversionStats(unittest.TestCase):
    """Test save_conversion_stats."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_stats_")
        self.conv = TableauToPowerBIConverter(
            os.path.join(self.tmpdir, "e"), os.path.join(self.tmpdir, "o")
        )
        self.conv.conversion_stats["objects_converted"] = {"a": 2, "b": 5}

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_stats_file_written(self):
        self.conv.save_conversion_stats()
        path = os.path.join(self.conv.output_dir, "conversion_stats.json")
        self.assertTrue(os.path.isfile(path))
        with open(path) as f:
            data = json.load(f)
        self.assertEqual(data["total"], 7)


class TestMain(unittest.TestCase):
    """Test main() entry point."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_main_")
        self.export_dir = os.path.join(self.tmpdir, "e")
        self.output_dir = os.path.join(self.tmpdir, "o")
        os.makedirs(self.export_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_main_with_sys_argv(self):
        import sys
        old_argv = sys.argv
        sys.argv = ["convert_all", self.export_dir, self.output_dir]
        try:
            main()
        finally:
            sys.argv = old_argv
        self.assertTrue(os.path.isdir(self.output_dir))


if __name__ == "__main__":
    unittest.main()
