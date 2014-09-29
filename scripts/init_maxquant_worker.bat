start /wait NDP452-KB2901907-x86-x64-AllOS-ENU.exe /passive
start /wait vcredist_x64-2008.exe /q
start /wait vcredist_x64-2010.exe /q
md c:\Users\Administrator\cygtemp
start /wait setup-x86_64.exe --site http://mirror.switch.ch/ftp/mirror/cygwin --packages python,procps,wget,openssh --local-package-dir c:\Users\Administrator\cygtemp --quiet-mode
c:\cygwin64\bin\bash -l -c "ssh-host-config --yes --cygwin \"tty ntsec\" --pwd arprthan"
net start sshd
netsh advfirewall firewall add rule name=SSH protocol=TCP localport=22 dir=in action=allow