 
#### Windows
https://dev.mysql.com/downloads/mysql/

Do dump
``````
# PowerShell


setx PATH "$env:PATH;C:\Program Files\MySQL\MySQL Server 8.0\bin"

# Restart PowerShell, then verify:

mysql --version

https://git-scm.com/download/win

git clone https://github.com/datacharmer/test_db.git
cd test_db

mysql -u root -p < employees.sql

# PowerShell sometimes blocks input redirection. Use this instead:

`cmd /c "mysql -u root -p < employees.sql"`
``````