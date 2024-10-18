@echo off

echo Iniciando el script...

set "PROJECT_DIR=C:\GIT\matriz_opp"

set "VENV_ACTIVATE=%PROJECT_DIR%\matriz_opp\Scripts\activate"

set "PYTHON_SCRIPT=%PROJECT_DIR%\main_v1.py"

set "TEMP_OUTPUT=%TEMP%\python_output.txt"

echo Verificando directorios y archivos...

if not exist "%PROJECT_DIR%" (

   echo Error: El directorio del proyecto no existe: %PROJECT_DIR%

   goto :error

)

if not exist "%VENV_ACTIVATE%" (

   echo Error: El archivo de activación del entorno virtual no existe: %VENV_ACTIVATE%

   goto :error

)

if not exist "%PYTHON_SCRIPT%" (

   echo Error: El script Python no existe: %PYTHON_SCRIPT%

   goto :error

)

echo Cambiando al directorio del proyecto...

cd /d "%PROJECT_DIR%"

echo Directorio actual: %CD%

echo Activando el entorno virtual...

call "%VENV_ACTIVATE%"

if errorlevel 1 (

   echo Error al activar el entorno virtual.

   goto :error

)

echo Ejecutando el script Python...

python "%PYTHON_SCRIPT%" > "%TEMP_OUTPUT%" 2>&1

if errorlevel 1 (

   echo Error al ejecutar el script Python.

   set "MENSAJE=Error al ejecutar el script Python. Salida:"

) else (

   echo Script Python ejecutado correctamente.

   set "MENSAJE=El script Python se ejecutó correctamente. Salida:"

)

:: Leer el contenido del archivo temporal

set "PYTHON_OUTPUT="

for /f "delims=" %%i in (%TEMP_OUTPUT%) do set "PYTHON_OUTPUT=!PYTHON_OUTPUT!%%i^n"

:: Mostrar el pop-up con la salida de Python

powershell -Command "Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.MessageBox]::Show('%MENSAJE%^n^n%PYTHON_OUTPUT%', 'Resultado del Script Python', 'OK', [System.Windows.Forms.MessageBoxIcon]::Information)"

:: Eliminar el archivo temporal

del "%TEMP_OUTPUT%"

echo Desactivando el entorno virtual...

deactivate

goto :eof

:error

echo Se produjo un error. Revisa los mensajes anteriores para más detalles.

msg %username% "Se produjo un error. Revisa la ventana de comando para más detalles."

pause

exit /b 1
 