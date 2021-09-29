pack config default-builder gcr.io/buildpacks/builder:v1

pack build clouddq --builder gcr.io/buildpacks/builder:v1 --env GOOGLE_ENTRYPOINT="python -m clouddq"