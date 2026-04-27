from config.project_config import ProjectConfig

# ----------------------------------------------------------------------
# 1. Test spreadsheet row offset helper
# ----------------------------------------------------------------------


def test_spreadsheet_row_number(project_config_strict: ProjectConfig):
    cfg = project_config_strict

    assert cfg.source is not None
    assert cfg.source.header_rows is not None

    cfg.source.header_rows = 5

    assert cfg.spreadsheet_row_number(0) == 5
    assert cfg.spreadsheet_row_number(10) == 15