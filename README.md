# Git 설정

1. Windows WSL2 설치 - https://hkim-data.tistory.com/17

2. Github Token 발급 - [[GitHub] 깃허브 토큰(Token) 생성하는 법 :: 대두코기](https://hoohaha.tistory.com/37)

3. git 환경 구축
- git clone
  
  ```
  git clone https://github.com/yms218/DB-Project.git
  ```
4. 파일/폴더 변경 후 commit 방법
- root 폴더로 이동한다 

- Untracked files : 변경된 파일/폴더 확인 작업 진행
  
  ```
  git status
  ```

- 변경 파일/폴더 현재 위치로부터 전부 add
  
  ```
  git add .   
  ```

- git status로 변경할 파일/폴더가 정상적으로 tracked 되었는지 확인
  
  ```
  git status
  ```

- 변경 사항 commit 
  
  ```
  git commit -m 'add each database project files'   
  ```

- main branch에 git push
  
  - Username : {본인 github id 입력}
  
  - Password : {본인 github 에서 발급받은 token}
  
  ```
  git push
  ```

- master 와 sync 맞춤
  
  ```
  git pull
  ```
