@echo off
setlocal
cd /d "%~dp0.."

where docker >nul 2>nul
if errorlevel 1 (
  echo Docker CLI bulunamadi. Docker Engine ve Compose v2 kurun.
  exit /b 1
)

docker compose version >nul 2>nul
if errorlevel 1 (
  echo Docker Compose v2 bulunamadi (docker compose).
  exit /b 1
)

if not exist .env (
  if exist .env.example (
    copy .env.example .env >nul
    echo .env.example dosyasindan .env olusturuldu. Gerekirse .env degerlerini duzenleyin.
  ) else (
    echo .env.example bulunamadi.
    exit /b 1
  )
)

if "%~1"=="" (
  docker compose -f compose.yaml -f compose.full.yaml --profile full up --build -d
) else (
  docker compose -f compose.yaml -f compose.full.yaml --profile full %*
)
exit /b %ERRORLEVEL%
