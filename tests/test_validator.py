"""Tests for fabric_import.validator"""

import json
import os
import shutil
import tempfile
import unittest

from fabric_import.validator import ArtifactValidator


class TestValidateArtifact(unittest.TestCase):
    """Tests for ArtifactValidator.validate_artifact()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_val_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_json_object(self):
        path = os.path.join(self.tmpdir, 'artifact.json')
        with open(path, 'w') as f:
            json.dump({'type': 'Dataset', 'name': 'test'}, f)
        valid, errors = ArtifactValidator.validate_artifact(path)
        self.assertTrue(valid)
        self.assertEqual(errors, [])

    def test_invalid_artifact_type(self):
        path = os.path.join(self.tmpdir, 'artifact.json')
        with open(path, 'w') as f:
            json.dump({'type': 'InvalidType'}, f)
        valid, errors = ArtifactValidator.validate_artifact(path)
        self.assertFalse(valid)
        self.assertTrue(any('Invalid artifact type' in e for e in errors))

    def test_file_not_found(self):
        valid, errors = ArtifactValidator.validate_artifact('/nonexistent.json')
        self.assertFalse(valid)
        self.assertTrue(any('not found' in e.lower() for e in errors))

    def test_invalid_json(self):
        path = os.path.join(self.tmpdir, 'bad.json')
        with open(path, 'w') as f:
            f.write('{not valid json}')
        valid, errors = ArtifactValidator.validate_artifact(path)
        self.assertFalse(valid)

    def test_non_object_json(self):
        path = os.path.join(self.tmpdir, 'list.json')
        with open(path, 'w') as f:
            json.dump([1, 2, 3], f)
        valid, errors = ArtifactValidator.validate_artifact(path)
        self.assertFalse(valid)

    def test_valid_types_accepted(self):
        for artifact_type in ArtifactValidator.VALID_ARTIFACT_TYPES:
            with self.subTest(artifact_type=artifact_type):
                path = os.path.join(self.tmpdir, f'{artifact_type}.json')
                with open(path, 'w') as f:
                    json.dump({'type': artifact_type}, f)
                valid, _ = ArtifactValidator.validate_artifact(path)
                self.assertTrue(valid)


class TestValidateJsonFile(unittest.TestCase):
    """Tests for ArtifactValidator.validate_json_file()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vj_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_json(self):
        path = os.path.join(self.tmpdir, 'valid.json')
        with open(path, 'w') as f:
            json.dump({'key': 'value'}, f)
        valid, err = ArtifactValidator.validate_json_file(path)
        self.assertTrue(valid)
        self.assertIsNone(err)

    def test_invalid_json(self):
        path = os.path.join(self.tmpdir, 'bad.json')
        with open(path, 'w') as f:
            f.write('not json')
        valid, err = ArtifactValidator.validate_json_file(path)
        self.assertFalse(valid)
        self.assertIsNotNone(err)


class TestValidateTmdlFile(unittest.TestCase):
    """Tests for ArtifactValidator.validate_tmdl_file()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vt_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_model_tmdl(self):
        path = os.path.join(self.tmpdir, 'model.tmdl')
        with open(path, 'w') as f:
            f.write('model Model\n  culture: en-US\n')
        valid, errors = ArtifactValidator.validate_tmdl_file(path)
        self.assertTrue(valid)

    def test_invalid_model_tmdl(self):
        path = os.path.join(self.tmpdir, 'model.tmdl')
        with open(path, 'w') as f:
            f.write('table SomeTable\n  columns:\n')
        valid, errors = ArtifactValidator.validate_tmdl_file(path)
        self.assertFalse(valid)

    def test_empty_tmdl(self):
        path = os.path.join(self.tmpdir, 'model.tmdl')
        with open(path, 'w') as f:
            f.write('')
        valid, errors = ArtifactValidator.validate_tmdl_file(path)
        self.assertFalse(valid)

    def test_non_model_tmdl_valid(self):
        path = os.path.join(self.tmpdir, 'orders.tmdl')
        with open(path, 'w') as f:
            f.write('table Orders\n  column OrderID\n')
        valid, errors = ArtifactValidator.validate_tmdl_file(path)
        self.assertTrue(valid)


class TestValidateNotebook(unittest.TestCase):
    """Tests for ArtifactValidator.validate_notebook()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vn_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_notebook(self):
        path = os.path.join(self.tmpdir, 'test.ipynb')
        nb = {
            'nbformat': 4,
            'metadata': {},
            'cells': [{'cell_type': 'code', 'source': ['print(1)']}],
        }
        with open(path, 'w') as f:
            json.dump(nb, f)
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertTrue(valid)

    def test_missing_cells(self):
        path = os.path.join(self.tmpdir, 'bad.ipynb')
        with open(path, 'w') as f:
            json.dump({'nbformat': 4, 'metadata': {}}, f)
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)

    def test_missing_metadata(self):
        path = os.path.join(self.tmpdir, 'bad2.ipynb')
        with open(path, 'w') as f:
            json.dump({'nbformat': 4, 'cells': []}, f)
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)

    def test_missing_nbformat(self):
        path = os.path.join(self.tmpdir, 'bad3.ipynb')
        with open(path, 'w') as f:
            json.dump({'metadata': {}, 'cells': []}, f)
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)


class TestValidateLakehouseDefinition(unittest.TestCase):
    """Tests for ArtifactValidator.validate_lakehouse_definition()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vlh_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_definition_legacy_format(self):
        path = os.path.join(self.tmpdir, 'lh.json')
        with open(path, 'w') as f:
            json.dump({'lakehouse_name': 'MyLH', 'tables': []}, f)
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertTrue(valid)

    def test_valid_definition_generator_format(self):
        path = os.path.join(self.tmpdir, 'lh.json')
        with open(path, 'w') as f:
            json.dump({
                'properties': {'displayName': 'MyLH'},
                'tables': [],
            }, f)
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertTrue(valid)

    def test_missing_tables(self):
        path = os.path.join(self.tmpdir, 'lh.json')
        with open(path, 'w') as f:
            json.dump({'lakehouse_name': 'MyLH'}, f)
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertFalse(valid)

    def test_missing_name_and_displayName(self):
        path = os.path.join(self.tmpdir, 'lh.json')
        with open(path, 'w') as f:
            json.dump({'tables': []}, f)
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertFalse(valid)


class TestValidateDataflowDefinition(unittest.TestCase):
    """Tests for ArtifactValidator.validate_dataflow_definition()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vdf_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_definition_legacy_format(self):
        path = os.path.join(self.tmpdir, 'df.json')
        with open(path, 'w') as f:
            json.dump({'mashup': 'section Section1;'}, f)
        valid, errors = ArtifactValidator.validate_dataflow_definition(path)
        self.assertTrue(valid)

    def test_valid_definition_generator_format(self):
        path = os.path.join(self.tmpdir, 'df.json')
        with open(path, 'w') as f:
            json.dump({'mashupDocument': 'section Section1;', 'queries': []}, f)
        valid, errors = ArtifactValidator.validate_dataflow_definition(path)
        self.assertTrue(valid)

    def test_missing_mashup(self):
        path = os.path.join(self.tmpdir, 'df.json')
        with open(path, 'w') as f:
            json.dump({'queries': []}, f)
        valid, errors = ArtifactValidator.validate_dataflow_definition(path)
        self.assertFalse(valid)


class TestValidatePipelineDefinition(unittest.TestCase):
    """Tests for ArtifactValidator.validate_pipeline_definition()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vpl_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_definition(self):
        path = os.path.join(self.tmpdir, 'pl.json')
        with open(path, 'w') as f:
            json.dump({
                'name': 'TestPipeline',
                'properties': {'activities': []},
            }, f)
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertTrue(valid)

    def test_missing_name(self):
        path = os.path.join(self.tmpdir, 'pl.json')
        with open(path, 'w') as f:
            json.dump({'properties': {'activities': []}}, f)
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)

    def test_missing_activities(self):
        path = os.path.join(self.tmpdir, 'pl.json')
        with open(path, 'w') as f:
            json.dump({'name': 'P', 'properties': {}}, f)
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)

    def test_activities_not_list(self):
        path = os.path.join(self.tmpdir, 'pl.json')
        with open(path, 'w') as f:
            json.dump({'name': 'P', 'properties': {'activities': 'bad'}}, f)
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)


class TestValidateProject(unittest.TestCase):
    """Tests for ArtifactValidator.validate_project()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vp_')
        self.project_dir = os.path.join(self.tmpdir, 'MyProject')
        os.makedirs(self.project_dir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_nonexistent_project(self):
        result = ArtifactValidator.validate_project('/nonexistent/path')
        self.assertFalse(result['valid'])

    def test_project_with_pbip(self):
        pbip_path = os.path.join(self.project_dir, 'MyProject.pbip')
        with open(pbip_path, 'w') as f:
            json.dump({'version': '1.0'}, f)
        result = ArtifactValidator.validate_project(self.project_dir)
        # At least the pbip is valid, even if other dirs are missing
        self.assertGreater(result['files_checked'], 0)


class TestValidateDirectory(unittest.TestCase):
    """Tests for ArtifactValidator.validate_directory()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vd_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_nonexistent_directory(self):
        result = ArtifactValidator.validate_directory('/nonexistent')
        self.assertEqual(result, {})

    def test_empty_directory(self):
        result = ArtifactValidator.validate_directory(self.tmpdir)
        self.assertEqual(result, {})

    def test_directory_with_json(self):
        path = os.path.join(self.tmpdir, 'test.json')
        with open(path, 'w') as f:
            json.dump({'type': 'Dataset'}, f)
        result = ArtifactValidator.validate_directory(self.tmpdir)
        self.assertIn('test.json', result)


if __name__ == '__main__':
    unittest.main()
