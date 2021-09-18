from pathlib import Path
import click

from clouddq.utils import get_template_file

DBT_TEMPLATED_FILE_LOCATIONS = {
    "dbt_project.yml": Path("dbt", "dbt_project.yml"),
    "main.sql": Path("dbt", "models", "data_quality_engine", "main.sql"),
    "main.sql": Path("dbt", "models", "data_quality_engine", "dq_summary.sql"),
}


def resolve_connection_configs(
    dbt_profiles_dir: str,
    environment_target: str,
    bigquery_oauth_connection_configs: str,
    bigquery_sa_keys_connection_configs: str,
    bigquery_sa_impersonation_connection_configs: str,
    debug: bool
):
    if dbt_profiles_dir:
        dbt_profiles_dir = Path(dbt_profiles_dir).absolute()
        if not dbt_profiles_dir.joinpath("profiles.yml").is_file():
            raise ValueError(
                f"Cannot find connection `profiles.yml` configurations at "
                f"`dbt_profiles_dir` path: {dbt_profiles_dir}"
            )
        if debug:
            click.secho(f"Using 'dbt_profiles_dir': {dbt_profiles_dir}", fg="yellow")

def resolve_dbt_path(
    dbt_path: str,
    debug: bool
) -> Path:
    if debug:
        click.secho(f"Current working directory: {Path().cwd()}", fg="yellow")
    if not dbt_path:
        dbt_path = Path().cwd().joinpath("dbt").absolute()
        if debug:
            click.secho(
                f"No argument 'dbt_path' provided. Defaulting to use "
                f"'dbt' directory in current working directory at: {dbt_path}",
                fg="magenta",
            )
    else:
        dbt_path = Path(dbt_path).absolute()
        if dbt_path.name != "dbt":
            dbt_path = dbt_path / "dbt"
    if not dbt_path.is_dir():
        if debug:
            click.secho(
                f"Creating a new dbt directory at 'dbt_path': {dbt_path}", fg="magenta"
            )
        dbt_path.mkdir(parents=True, exist_ok=True)
    if debug:
        click.secho(f"Using 'dbt_path': {dbt_path}", fg="yellow")
    return dbt_path


def prepare_dbt_directory(
    dbt_path: str,
    debug: bool
) -> str:
    dbt_project_path = dbt_path.absolute().joinpath("dbt_project.yml")
    if not dbt_project_path.is_file():
        if debug:
            click.secho(
                f"Cannot find `dbt_project.yml` configurations in current path: "
                f"{dbt_project_path}",
                fg="magenta",
            )
            click.secho(
                f"Writing templated 'dbt_project.yml' to: {dbt_project_path} ",
                fg="magenta",
            )
        write_templated_file_to_path(dbt_project_path, DBT_TEMPLATED_FILE_LOCATIONS)
    click.secho(f"Using 'dbt_project_path': {dbt_project_path}", fg="yellow")
    dbt_main_path = dbt_path / "models" / "data_quality_engine"
    dbt_main_path.mkdir(parents=True, exist_ok=True)
    if not dbt_main_path.joinpath("main.sql").is_file():
        write_templated_file_to_path(dbt_main_path.joinpath("main.sql"), DBT_TEMPLATED_FILE_LOCATIONS)
    if not dbt_main_path.joinpath("dq_summary.sql").is_file():
        write_templated_file_to_path(dbt_main_path.joinpath("dq_summary.sql"), DBT_TEMPLATED_FILE_LOCATIONS)
    dbt_rule_binding_views_path = dbt_path / "models" / "rule_binding_views"
    dbt_rule_binding_views_path.mkdir(parents=True, exist_ok=True)
    click.secho(
        f"Writing generated sql to {dbt_rule_binding_views_path.absolute()}/",
        fg="yellow",
    )
    return dbt_rule_binding_views_path
