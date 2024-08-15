## How to Run the Program



### Step 1: Download git
[gitUrl](https://git-scm.com/download/win)
This is so you can clone this repository


### Step 2: Download node
[nodeUrl](https://nodejs.org/en/download/prebuilt-installer)
This is so you can clone this repository


### Step 3: open your terminal & clone the Repository
cd to the directory you want to place this program:
```bash
cd Documents/
```
```bash
git clone https://github.com/seanbb88/StephenSports.git
cd Documents/StephenSports
```

### Step 4: Change database credentials
At the top of the index.js file change the local database credentials to your own

```bash
const dbConfig = {
    user: 'postgres',
    host: 'localhost',
    database: 'sports',
    password: 'password',
    port: 5432,
}
```

### Step 5: Install dependencies
Now in your terminal run the following command, this will download the program dependencies

```bash
npm install
```

### Step 6: Run Program
Run the program, you will see some logging in the terminal and after its done you should have data in your db
```bash
npm start
```
