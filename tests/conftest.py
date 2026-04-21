from pathlib import Path
import pytest
from config.project_config import ProjectConfig
from schemas.column_definitions import ColumnDefinition
from src.context import ETLContext
from src.extractor import Extractor
from schemas.table_schema import TableSchema


@pytest.fixture
def extractor(project_config, etl_context):
    return Extractor(project_config=project_config, etl_context=etl_context)


@pytest.fixture
def project_config():
    # just reads the yaml file
    config_path = Path(__file__).parent / "data" / "project.yaml"

    return ProjectConfig.from_yaml(config_path)


@pytest.fixture
def project_config_strict():
    # read the yaml file and set to strict validation
    config_path = Path(__file__).parent / "data" / "project.yaml"
    pc = ProjectConfig.from_yaml(config_path)
    assert pc.runtime is not None
    pc.runtime.strict_validation = True
    return pc


@pytest.fixture
def project_config_permissive():
    # read the yaml file and set to permissive validation
    config_path = Path(__file__).parent / "data" / "project.yaml"
    pc = ProjectConfig.from_yaml(config_path)
    assert pc.runtime is not None
    pc.runtime.strict_validation = False
    return pc


@pytest.fixture
def table_schema_default():
    # default table schema
    schema = TableSchema()
    schema.table_name = "Test"
    schema.columns = [
        ColumnDefinition(
            column_name="Name",
            source_column="full_name",
            data_type="str",
            null_allowed=True,
        ),
        ColumnDefinition(
            column_name="Email",
            source_column="email_address",
            data_type="str",
            null_allowed=True,
        ),
    ]
    return schema


@pytest.fixture
def etl_context():
    ctx = ETLContext(
        log_dir=Path("."),
        data_dir=Path("."),
        output_dir=Path("."),
        config_dir=Path("."),
        cleaned_suffix="_clean",
        currency_symbol="R",
    )
    ctx.datetime_parser = ctx.get_datetime_parser(None)
    return ctx
