# Build the app image and (re)deploy everything to the local Docker Desktop cluster.
#   .\scripts\deploy.ps1            build + apply + restart app pods
#   .\scripts\deploy.ps1 -NoBuild   just apply manifests
param([switch]$NoBuild)
# Not "Stop": in Windows PowerShell 5.1 any stderr output from docker/kubectl (e.g. build
# progress) would abort the script. We check $LASTEXITCODE explicitly instead.
$ErrorActionPreference = "Continue"
Set-Location (Split-Path $PSScriptRoot -Parent)

kubectl config use-context docker-desktop | Out-Null

if (-not $NoBuild) {
    docker build -q -t wqa-app:dev .
    if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
    docker build -q -t wqa-spark:dev -f spark/Dockerfile .
    if ($LASTEXITCODE -ne 0) { throw "spark image build failed" }
}

kubectl apply -k k8s/base
if ($LASTEXITCODE -ne 0) { throw "kubectl apply failed" }

if (-not $NoBuild) {
    # Same tag every time, so force pods to pick up the fresh image.
    kubectl -n wqa rollout restart deployment -l part-of=wqa-app
    # SparkApplication: deleting the driver makes the operator resubmit it with the new image.
    kubectl -n wqa delete pod -l "spark-role=driver,sparkoperator.k8s.io/app-name=wqa-fetcher" --ignore-not-found
}

kubectl -n wqa wait kafka/wqa --for=condition=Ready --timeout=300s
kubectl -n wqa get pods
Write-Host "`nUI: http://localhost:8000  (or: kubectl -n wqa port-forward svc/api 8000:8000)"
