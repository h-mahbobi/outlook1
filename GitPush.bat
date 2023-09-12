git init
git remote add origin https://github.com/h-mahbobi/outlook1.git
git pull origin master


git add .
git status
set /p input= write commit message : 
git commit -m  "%input%"
git push origin master
