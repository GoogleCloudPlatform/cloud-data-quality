from pathlib import Path
from typing import Dict, Optional

import logging
logger = logging.getLogger(__name__)

from clouddq.lib import load_jinja_template
from clouddq.utils import write_templated_file_to_path
from clouddq.runners.dbt.dbt_connection_configs import DbtConnectionConfig, GcpDbtConnectionConfig
from clouddq.runners.dbt.dbt_utils import run_dbt, resolve_dbt_path


DBT_TEMPLATED_FILE_LOCATIONS = {
    "profiles.yml": Path("dbt", "profiles.yml"),
    "dbt_project.yml": Path("dbt", "dbt_project.yml"),
    "main.sql": Path("dbt", "models", "data_quality_engine", "main.sql"),
    "main.sql": Path("dbt", "models", "data_quality_engine", "dq_summary.sql"),
}

class DbtRunner:
    dbt_path: Path
    dbt_profiles_dir: Path
    environment_target: str
    connection_config: DbtConnectionConfig
    dbt_rule_binding_views_path: Path
    debug: bool

    def __init__(self,
        dbt_path: Optional[Path],
        dbt_profiles_dir: Optional[str],
        environment_target: Optional[str],
        gcp_oauth_connection_configs: Optional[str],
        gcp_sa_keys_connection_configs: Optional[str],
        gcp_sa_impersonation_connection_configs: Optional[str],
        create_paths_if_not_exists: bool = True,
        debug: bool = False
        ):
        self.debug = debug
         # Prepare local dbt environment
        self.dbt_path = self.__resolve_dbt_path(
            dbt_path=dbt_path,
            create_paths_if_not_exists=create_paths_if_not_exists,
            debug=debug)
        self.__prepare_dbt_project_path()
        self.__prepare_dbt_main_path()
        self.__prepare_rule_binding_view_path()
        # Prepare connection configurations
        self.connection_config = self.__resolve_connection_configs(
            dbt_profiles_dir=dbt_profiles_dir,
            environment_target=environment_target,
            gcp_oauth_connection_configs=gcp_oauth_connection_configs,
            gcp_sa_keys_connection_configs=gcp_sa_keys_connection_configs,
            gcp_sa_impersonation_connection_configs=gcp_sa_impersonation_connection_configs,
        )
        self.__write_connection_configs_to_dbt_profiles_yml_file()

    def run(self, configs: Dict, debug: bool = False, dry_run: bool = False):
        logger.info("Running dbt in path: %s", self.dbt_path)
        run_dbt(
            dbt_path=self.dbt_path,
            dbt_profile_dir=self.dbt_profiles_dir,
            configs=configs,
            environment=self.environment_target,
            debug=debug,
            dry_run=dry_run,
        )

    def __resolve_connection_configs(
        self,
        dbt_profiles_dir: Optional[str],
        environment_target: Optional[str],
        gcp_oauth_connection_configs: Optional[str],
        gcp_sa_keys_connection_configs: Optional[str],
        gcp_sa_impersonation_connection_configs: Optional[str],
        debug: bool
    ) -> DbtConnectionConfig:
        if dbt_profiles_dir:
            dbt_profiles_dir = Path(dbt_profiles_dir).absolute()
            if not dbt_profiles_dir.joinpath("profiles.yml").is_file():
                raise ValueError(
                    f"Cannot find connection `profiles.yml` configurations at "
                    f"`dbt_profiles_dir` path: {dbt_profiles_dir}"
                )
            self.dbt_profiles_dir = dbt_profiles_dir
            self.environment_target = environment_target
        elif environment_target:
            logger.warn(
                "Unless `dbt_profiles_dir` is defined, the following argument "
                "for `environment_target` will be ignored: %s",
                environment_target
            )
        else:
            # create GcpDbtConnectionConfig
            connection_arguments = [
                gcp_oauth_connection_configs,
                gcp_sa_keys_connection_configs,
                gcp_sa_impersonation_connection_configs]
            if len([arg for arg in connection_arguments if arg]) != 1:
                raise ValueError(
                    "Exactly one GCP connection config must be specified from %s",
                    connection_arguments
                    )
            elif gcp_oauth_connection_configs:
                (project_id,
                gcp_region,
                dataset_id,
                ) = gcp_oauth_connection_configs
                connection_config = GcpDbtConnectionConfig(
                    project_id=project_id,
                    gcp_region=gcp_region,
                    dataset_id=dataset_id,
                )
            elif gcp_sa_keys_connection_configs:
                (project_id,
                gcp_region,
                dataset_id,
                service_account_key_path,
                ) = gcp_sa_keys_connection_configs
                connection_config = GcpDbtConnectionConfig(
                    project_id=project_id,
                    gcp_region=gcp_region,
                    dataset_id=dataset_id,
                    service_account_key_path=service_account_key_path,
                )
            elif gcp_sa_impersonation_connection_configs:
                (project_id,
                gcp_region,
                dataset_id,
                impersonation_credentials
                ) = gcp_sa_impersonation_connection_configs
                connection_config = GcpDbtConnectionConfig(
                    project_id=project_id,
                    gcp_region=gcp_region,
                    dataset_id=dataset_id,
                    impersonation_credentials=impersonation_credentials
                )
            self.connection_config = connection_config
            self.dbt_profiles_dir = self.dbt_path
        if debug:
            logger.debug("Using 'dbt_profiles_dir': %s", dbt_profiles_dir)
        # todo: test dbt connection
        pass

    def __resolve_dbt_path(
        self,
        dbt_path: str,
        create_paths_if_not_exists: bool,
        debug: bool
        ) -> Path:
        if debug:
            logger.info("Current working directory: %s", Path().cwd())
        if not dbt_path:
            dbt_path = Path().cwd().joinpath("dbt").absolute()
            if debug:
                logger.warn(
                    "No argument 'dbt_path' provided. Defaulting to use "
                    "'dbt' directory in current working directory at: %s",
                    dbt_path,
                )
        else:
            dbt_path = Path(dbt_path).absolute()
            if dbt_path.name != "dbt":
                dbt_path = dbt_path / "dbt"
        if not dbt_path.is_dir():
            if create_paths_if_not_exists:
                if debug:
                    logger.warn(
                        "Creating a new dbt directory at 'dbt_path': %s", dbt_path
                    )
                dbt_path.mkdir(parents=True, exist_ok=True)
            else:
                raise ValueError(
                    "Provided 'dbt_path' does not exists: %s", dbt_path
                )
        if debug:
            logger.info(f"Using 'dbt_path': {dbt_path}", fg="yellow")
        return dbt_path

    def __prepare_dbt_project_path(self) -> None:
        dbt_project_path = self.dbt_path.absolute().joinpath("dbt_project.yml")
        if not dbt_project_path.is_file():
            if self.debug:
                logger.warn(
                    "Cannot find `dbt_project.yml` configurations in current path: %s",
                    dbt_project_path,
                )
                logger.warn(
                    "Writing templated 'dbt_project.yml' to: %s",
                    dbt_project_path,
                )
            write_templated_file_to_path(dbt_project_path, DBT_TEMPLATED_FILE_LOCATIONS)
        logger.info("Using 'dbt_project_path': %s", dbt_project_path)

    def __prepare_dbt_main_path(self) -> None:
        assert self.dbt_path.is_dir()
        dbt_main_path = self.dbt_path / "models" / "data_quality_engine"
        dbt_main_path.mkdir(parents=True, exist_ok=True)
        if not dbt_main_path.joinpath("main.sql").is_file():
            write_templated_file_to_path(dbt_main_path.joinpath("main.sql"), DBT_TEMPLATED_FILE_LOCATIONS)
        if not dbt_main_path.joinpath("dq_summary.sql").is_file():
            write_templated_file_to_path(dbt_main_path.joinpath("dq_summary.sql"), DBT_TEMPLATED_FILE_LOCATIONS)

    def __prepare_rule_binding_view_path(self) -> None:
        assert self.dbt_path.is_dir()
        self.dbt_rule_binding_views_path = self.dbt_path / "models" / "rule_binding_views"
        self.dbt_rule_binding_views_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Writing generated sql to %s/",
            self.dbt_rule_binding_views_path.absolute(),
        )

    def __write_connection_configs_to_dbt_profiles_yml_file(self) -> None:
        template = load_jinja_template(
            template="profiles.yml",
            template_parent=DBT_TEMPLATED_FILE_LOCATIONS["profiles.yml"])
        dbt_profiles_configs = self.connection_config.to_dbt_profiles_dict()
        dbt_profiles_generated = template.render(dbt_profiles_configs)
        logging.warn(
            "Writing user input GCP connection profile to dbt profiles.yml at path:\n%s",
            self.dbt_profiles_dir
        )
        self.dbt_profiles_dir.write_text(dbt_profiles_generated)