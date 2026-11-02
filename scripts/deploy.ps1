# Build the app image and (re)deploy everything to the local Docker Desktop cluster.
#   .\scripts\deploy.ps1            build + apply + restart app pods
#   .\scripts\deploy.ps1 -NoBuild   just apply manifests
param([switch]$NoBuild)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

kubectl config use-context docker-desktop | Out-Null

if (-not $NoBuild) {
    docker build -t wqa-app:dev .
    if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
}

kubectl apply -k k8s/base
if ($LASTEXITCODE -ne 0) { throw "kubectl apply failed" }

if (-not $NoBuild) {
    # Same tag every time, so force pods to pick up the fresh image.
    kubectl -n wqa rollout restart deployment -l part-of=wqa-app
}

kubectl -n wqa rollout status statefulset/kafka --timeout=180s
kubectl -n wqa get pods
Write-Host "`nUI: http://localhost:8000  (or: kubectl -n wqa port-forward svc/api 8000:8000)"
