# DATA LAKEHOUSE WORKFLOW


<img width="2451" alt="data lake workflow" src="https://github.com/user-attachments/assets/6a8b8c65-4c7c-4abf-a470-fb0e6336bc86" />




1. input file di source folder
2. organize.py untuk mengorganisasikan file untuk masuk ke dalam folder raw kemudian dimasukkan sesuai dengan type file(dalam raw ada folder txt, csv dan pdf)
3. analyze.py, output dimasukkan ke dalam folder structured dan dimasukkan lagi ke dalam folder sesuai tipe file (dalam structured ada folder txt, csv dan pdf), lakukan treatment khusus pada setiap tipe file
4. passtostg.py, load structure pada setiap tipe file ke dalam db staging
5. transform.py, dari dimensi tabel yang sudah ada bentuk star schema
6. passtodw.py, load star schema ke dalam dw
