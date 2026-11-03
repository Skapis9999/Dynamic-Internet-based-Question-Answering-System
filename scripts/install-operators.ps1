# One-time setup: install the Kubernetes operators this project relies on (pinned versions).
#   Strimzi 1.3.0         - runs Kafka (Kafka, KafkaNodePool, KafkaTopic resources), namespace wqa
#   Spark Operator 2.5.2  - runs Spark jobs (SparkApplication resources), namespace spark-operator
#   KEDA 2.21.0           - autoscales workers on Kafka consumer lag, namespace keda
# Requires kubectl and helm. Safe to re-run.
$ErrorActionPreference = "Continue"
$STRIMZI = "1.3.0"; $SPARK_OP = "2.5.2"; $KEDA = "2.21.0"
kubectl config use-context docker-desktop | Out-Null

# A fresh `winget install Helm.Helm` isn't on PATH until a new shell; find it anyway.
$helm = (Get-Command helm -ErrorAction SilentlyContinue).Source
if (-not $helm) {
    $helm = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Helm.Helm_*\*\helm.exe" -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $helm) { throw "helm not found: winget install Helm.Helm" }

# Namespace first (the Strimzi operator lives in, and watches, wqa)
kubectl create namespace wqa --dry-run=client -o yaml | kubectl apply -f -

Write-Host "== Strimzi $STRIMZI"
$tmp = Join-Path $env:TEMP "strimzi-$STRIMZI.yaml"
Invoke-WebRequest -UseBasicParsing -OutFile $tmp "https://github.com/strimzi/strimzi-kafka-operator/releases/download/$STRIMZI/strimzi-cluster-operator-$STRIMZI.yaml"
(Get-Content $tmp -Raw) -replace 'namespace: myproject', 'namespace: wqa' | Set-Content -Encoding utf8 $tmp
# Server-side apply: the CRDs are too large for client-side apply's annotation.
kubectl apply --server-side --force-conflicts -n wqa -f $tmp
kubectl -n wqa set resources deploy/strimzi-cluster-operator --requests=memory=256Mi,cpu=100m --limits=memory=384Mi

Write-Host "== Spark Operator $SPARK_OP"
& $helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>$null | Out-Null
& $helm repo update spark-operator | Out-Null
& $helm upgrade --install spark-operator spark-operator/spark-operator --version $SPARK_OP `
    --namespace spark-operator --create-namespace `
    --set "spark.jobNamespaces={wqa}" `
    --set controller.resources.requests.memory=128Mi --set controller.resources.limits.memory=256Mi `
    --set webhook.resources.requests.memory=64Mi --set webhook.resources.limits.memory=128Mi `
    --wait

Write-Host "== KEDA $KEDA"
kubectl apply --server-side --force-conflicts -f "https://github.com/kedacore/keda/releases/download/v$KEDA/keda-$KEDA.yaml"

kubectl -n wqa rollout status deploy/strimzi-cluster-operator --timeout=180s
kubectl -n keda rollout status deploy/keda-operator --timeout=180s
kubectl get pods -n wqa -l name=strimzi-cluster-operator
kubectl get pods -n spark-operator
kubectl get pods -n keda
