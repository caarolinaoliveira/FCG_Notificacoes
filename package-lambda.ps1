# Publica o FCG.Worker e empacota o zip que o LocalStack usa para registrar
# as funcoes Lambda (localstack-init/00-setup-notificacoes.sh). Rode este
# script sempre que o codigo do FCG.Worker mudar, antes de subir/reiniciar
# o docker-compose da orquestracao.
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$workerProject = Join-Path $root "FCG.Worker\FCG.Worker.csproj"
$publishDir = Join-Path $root "FCG.Worker\bin\Release\net8.0\linux-x64\publish"
$artifactsDir = Join-Path $root "..\FCG_Orquestracao\localstack-init\artifacts"
$zipPath = Join-Path $artifactsDir "notificacoes.zip"

Write-Host "Publicando FCG.Worker (Release, linux-x64)..."
dotnet publish $workerProject -c Release -r linux-x64 --self-contained false -o $publishDir
if ($LASTEXITCODE -ne 0) { throw "dotnet publish falhou." }

if (-not (Test-Path $artifactsDir)) {
    New-Item -ItemType Directory -Force -Path $artifactsDir | Out-Null
}

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

Write-Host "Empacotando em $zipPath..."
Compress-Archive -Path (Join-Path $publishDir "*") -DestinationPath $zipPath

Write-Host "Pronto. Suba (docker-compose up) ou reinicie o LocalStack (docker-compose restart localstack) para registrar a Lambda."
