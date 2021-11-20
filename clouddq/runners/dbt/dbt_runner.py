# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from pathlib import Path
from typing import Dict
from typing import Optional

from clouddq.runners.dbt.dbt_connection_configs import DEFAULT_DBT_ENVIRONMENT_TARGET
from clouddq.runners.dbt.dbt_connection_configs import DbtConnectionConfig
from clouddq.runners.dbt.dbt_connection_configs import GcpDbtConnectionConfig
from clouddq.runners.dbt.dbt_utils import JobStatus
from clouddq.runners.dbt.dbt_utils import run_dbt
from clouddq.utils import write_templated_file_to_path


logger = logging.getLogger(__name__)

DBT_TEMPLATED_FILE_LOCATIONS = {
    "profiles.yml": Path("dbt", "profiles.yml"),
    "dbt_project.yml": Path("dbt", "dbt_project.yml"),
    "main.sql": Path("dbt", "models", "data_quality_engine", "main.sql"),
    "dq_summary.sql": Path("dbt", "models", "data_quality_engine", "dq_summary.sql"),
}


class DbtRunner:
    dbt_path: Path
    dbt_profiles_dir: Path
    environment_target: str
    connection_config: DbtConnectionConfig
    dbt_rule_binding_views_path: Path

    def __init__(
        self,
        dbt_path: Optional[Path],
        dbt_profiles_dir: Optional[str],
        environment_target: Optional[str],
        gcp_project_id: Optional[str],
        gcp_region_id: Optional[str],
        gcp_bq_dataset_id: Optional[str],
        gcp_service_account_key_path: Optional[Path],
        gcp_impersonation_credentials: Optional[str],
        create_paths_if_not_exists: bool = True,
    ):
        # Prepare local dbt environment
        self.dbt_path = self.__resolve_dbt_path(
            dbt_path=dbt_path,
            create_paths_if_not_exists=create_paths_if_not_exists,
            write_log=True,
        )
        self.__prepare_dbt_project_path()
        self.__prepare_dbt_main_path()
        self.__prepare_rule_binding_view_path(write_log=True)
        # Prepare connection configurations
        self.__resolve_connection_configs(
            dbt_profiles_dir=dbt_profiles_dir,
            environment_target=environment_target,
            gcp_project_id=gcp_project_id,
            gcp_region_id=gcp_region_id,
            gcp_bq_dataset_id=gcp_bq_dataset_id,
            gcp_service_account_key_path=gcp_service_account_key_path,
            gcp_impersonation_credentials=gcp_impersonation_credentials,
        )
        logger.debug(f"Using 'dbt_profiles_dir': {self.dbt_profiles_dir}")

    def run(
        self, configs: Dict, debug: bool = False, dry_run: bool = False
    ) -> JobStatus:
        logger.debug(f"Running dbt in path: {self.dbt_path}")
        if debug:
            self.test_dbt_connection()
        job_status = run_dbt(
            dbt_path=self.dbt_path,
            dbt_profile_dir=self.dbt_profiles_dir,
            configs=configs,
            environment=self.environment_target,
            debug=False,
            dry_run=dry_run,
        )
        return job_status

    def test_dbt_connection(self):
        run_dbt(
            dbt_path=self.dbt_path,
            dbt_profile_dir=self.dbt_profiles_dir,
            environment=self.environment_target,
            debug=True,
            dry_run=True,
        )

    def get_dbt_path(self) -> Path:
        self.__resolve_dbt_path(self.dbt_path)
        return Path(self.dbt_path)

    def get_rule_binding_view_path(self) -> Path:
        self.__prepare_rule_binding_view_path()
        return Path(self.dbt_rule_binding_views_path)

    def get_dbt_profiles_dir(self) -> Path:
        self.__resolve_connection_configs(
            dbt_profiles_dir=self.dbt_profiles_dir,
            environment_target=self.environment_target,
        )
        return Path(self.dbt_profiles_dir)

    def get_dbt_environment_target(self) -> str:
        self.__resolve_connection_configs(
            dbt_profiles_dir=self.dbt_profiles_dir,
            environment_target=self.environment_target,
        )
        return self.environment_target

    def __resolve_connection_configs(
        self,
        dbt_profiles_dir: Optional[str],
        environment_target: Optional[str],
        gcp_project_id: Optional[str] = None,
        gcp_region_id: Optional[str] = None,
        gcp_bq_dataset_id: Optional[str] = None,
        gcp_service_account_key_path: Optional[Path] = None,
        gcp_impersonation_credentials: Optional[str] = None,
    ) -> None:
        if dbt_profiles_dir:
            dbt_profiles_dir = Path(dbt_profiles_dir).absolute()
            if not dbt_profiles_dir.joinpath("profiles.yml").is_file():
                raise ValueError(
                    f"Cannot find connection `profiles.yml` configurations at "
                    f"`dbt_profiles_dir` path: {dbt_profiles_dir}"
                )
            self.dbt_profiles_dir = dbt_profiles_dir
            self.environment_target = environment_target
        else:
            # create GcpDbtConnectionConfig
            connection_config = GcpDbtConnectionConfig(
                gcp_project_id=gcp_project_id,
                gcp_region_id=gcp_region_id,
                gcp_bq_dataset_id=gcp_bq_dataset_id,
                gcp_service_account_key_path=gcp_service_account_key_path,
                gcp_impersonation_credentials=gcp_impersonation_credentials,
            )
            self.connection_config = connection_config
            self.dbt_profiles_dir = Path(self.dbt_path)
            logger.debug(
                "Writing user input GCP connection profile to dbt profiles.yml "
                f"at path: {self.dbt_profiles_dir}",
            )
            if environment_target:
                logger.debug(f"Using `environment_target`: {environment_target}")
                self.environment_target = environment_target
                self.connection_config.to_dbt_profiles_yml(
                    target_directory=self.dbt_profiles_dir,
                    environment_target=self.environment_target,
                )
            else:
                self.environment_target = DEFAULT_DBT_ENVIRONMENT_TARGET
                self.connection_config.to_dbt_profiles_yml(
                    target_directory=self.dbt_profiles_dir
                )

    def __resolve_dbt_path(
        self,
        dbt_path: str,
        create_paths_if_not_exists: bool = False,
        write_log: bool = False,
    ) -> Path:
        logger.debug(f"Current working directory: {Path().cwd()}")
        if not dbt_path:
            dbt_path = Path().cwd().joinpath("dbt").absolute()
            logger.debug(
                "No argument 'dbt_path' provided. Defaulting to use "
                f"'dbt' directory in current working directory at: {dbt_path}"
            )
        else:
            dbt_path = Path(dbt_path).absolute()
            if dbt_path.name != "dbt":
                dbt_path = dbt_path / "dbt"
        if not dbt_path.is_dir():
            if create_paths_if_not_exists:
                logger.debug(f"Creating a new dbt directory at 'dbt_path': {dbt_path}")
                dbt_path.mkdir(parents=True, exist_ok=True)
            else:
                raise ValueError(f"Provided 'dbt_path' does not exists: {dbt_path}")
        if write_log:
            logger.debug(f"Using 'dbt_path': {dbt_path}")
        return dbt_path

    def __prepare_dbt_project_path(self) -> None:
        dbt_project_path = self.dbt_path.absolute().joinpath("dbt_project.yml")
        if not dbt_project_path.is_file():
            logger.debug(
                f"Cannot find `dbt_project.yml` in path: {dbt_project_path} \n"
                f"Writing templated file to: {dbt_project_path}/dbt_project.yml"
            )
            write_templated_file_to_path(dbt_project_path, DBT_TEMPLATED_FILE_LOCATIONS)
        logger.debug(f"Using 'dbt_project_path': {dbt_project_path}")

    def __prepare_dbt_main_path(self) -> None:
        assert self.dbt_path.is_dir()
        dbt_main_path = self.dbt_path / "models" / "data_quality_engine"
        dbt_main_path.mkdir(parents=True, exist_ok=True)
        write_templated_file_to_path(
            dbt_main_path.joinpath("main.sql"), DBT_TEMPLATED_FILE_LOCATIONS
        )
        write_templated_file_to_path(
            dbt_main_path.joinpath("dq_summary.sql"), DBT_TEMPLATED_FILE_LOCATIONS
        )

    def __prepare_rule_binding_view_path(self, write_log: bool = False) -> None:
        assert self.dbt_path.is_dir()
        self.dbt_rule_binding_views_path = (
            self.dbt_path / "models" / "rule_binding_views"
        )
        self.dbt_rule_binding_views_path.mkdir(parents=True, exist_ok=True)
        if write_log:
            logger.debug(
                "Writing generated sql to "
                f"{self.dbt_rule_binding_views_path.absolute()}/",
            )
