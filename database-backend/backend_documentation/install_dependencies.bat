@echo off
echo ==========================================
echo Installing Stratification Dependencies
echo ==========================================

echo.
echo Installing required Python packages...
echo.

echo Installing pandas...
pip install pandas==2.1.3

echo Installing numpy...
pip install numpy==1.24.4

echo Installing scikit-learn...
pip install scikit-learn==1.3.2

echo Installing scipy...
pip install scipy==1.11.4

echo.
echo ==========================================
echo Installation Complete!
echo ==========================================

echo.
echo Testing imports...
python -c "import pandas; print('✅ pandas:', pandas.__version__)"
python -c "import numpy; print('✅ numpy:', numpy.__version__)"
python -c "import sklearn; print('✅ scikit-learn:', sklearn.__version__)"
python -c "import scipy; print('✅ scipy:', scipy.__version__)"

echo.
echo All dependencies installed successfully!
echo You can now restart the backend server.
echo.
pause 