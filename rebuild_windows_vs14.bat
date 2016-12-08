set VISUALSTUDIO_VERSION_MAJOR=14
ECHO ===== CMake for 64-bit ======
call "C:\Program Files (x86)\Microsoft Visual Studio %VISUALSTUDIO_VERSION_MAJOR%.0\VC\vcvarsall.bat" amd64


echo BUILDING
rmdir /s /q build
mkdir build 
cd build
cmake -G "Visual Studio %VISUALSTUDIO_VERSION_MAJOR% Win64"  ..
msbuild ALL_BUILD.vcxproj /p:Configuration=Debug /p:Platform=x64 /maxcpucount:12
msbuild ALL_BUILD.vcxproj /p:Configuration=Release /p:Platform=x64 /maxcpucount:12
cd ..
