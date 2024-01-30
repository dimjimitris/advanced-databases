# Εξαμηνιαία εργασία για το μάθημα "Προχωρημένα Θέματα Βάσεων Δεδομένων"

## Μέλη ομάδας:
* Δημήτριος Γεωργούσης, el19005
* Γεώργιος Αλέξιος Καπετανάκης, el19062

## Περιεχόμενα αποθετηρίου
* `src/`: περιέχει όλο τον κώδικα που γράψαμε (queries και scripts για setup)
* `query_outputs/`: περιέχει τα αποτελέσματα των queries
* `query_plans/`: περιέχει τα πλάνα εκτέλεσης των queries (σε μορφή κειμένου αλλά και γραφικών από το Spark UI)
* `spark_outputs/`: περιέχει το output του Spark για κάθε query
* `03119005_03119062.pdf`: η αναφορά της εργασίας

## Περιβάλλον ανάπτυξης
Το περιβάλλον ανάπτυξης και δοκιμών του λογισμικού στα δύο μηχανήματα μας είναι όπως το παρακάτω:


```
user@okeanos-master:~$ lsb_release -a
No LSB modules are available.
Distributor ID: Ubuntu
Description:    Ubuntu 16.04.3 LTS
Release:        16.04
Codename:       xenial
user@okeanos-master:~$ java -version
openjdk version "1.8.0_292"
OpenJDK Runtime Environment (build 1.8.0_292-8u292-b10-0ubuntu1~16.04.1-b10)
OpenJDK 64-Bit Server VM (build 25.292-b10, mixed mode)
user@okeanos-master:~$ python --version
Python 3.8.18
user@okeanos-master:~$ spark-submit --version
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 1.8.0_292
Branch HEAD
Compiled by user ubuntu on 2023-09-09T01:53:20Z
Revision ce5ddad990373636e94071e7cef2f31021add07b
Url https://github.com/apache/spark
Type --help for more information.
user@okeanos-master:~$ hadoop version
Hadoop 3.3.6
Source code repository https://github.com/apache/hadoop.git -r 1be78238728da9266a4f88195058f08fd012bf9c
Compiled by ubuntu on 2023-06-18T08:22Z
Compiled on platform linux-x86_64
Compiled with protoc 3.7.1
From source with checksum 5652179ad55f76cb287d9c633bb53bbd
This command was run using /home/user/opt/bin/hadoop-3.3.6/share/hadoop/common/hadoop-common-3.3.6.jar
```

## Setup
* Οι παρακάτω οδηγίες έχουν δοκιμαστεί ΜΟΝΟ σε Ubuntu 16.04 Server του okeanos-knossos. Τα παρακάτω βήματα υποθέτουν ότι δύο τέτοια μηχανήματα έχουν δημιουργηθεί και ότι το τρέχον αποθετήριο έχει γίνει `git clone` και στα δύο.

1. Εκτελούμε το script `src/shell_scripts/setup_vm.sh`΄και στα δύο μηχανήματα. Δίνουμε ως πρώτο argument είτε `master`, είτε `worker`, αναλόγως με το ποιό μηχάνημα θέλουμε να φτιάξουμε. Δίνουμε ως δεύτερο argument την public IPv4 του `master`.
2. Εκτελούμε και στα δύο μηχανήματα:
   * `ssh-keygen`
   * `ssh-copy-id -i ~/.ssh/id_rsa.pub okeanos-master`
   * `ssh-copy-id -i ~/.ssh/id_rsa.pub okeanos-worker`
4. Εκτελούμε το script `src/shell_scripts/format_hdfs.sh` μόνο στον `master`. Τα μηχανήματα είναι πλέον έτοιμα για να τρέξουν Hadoop/Spark.
5. Εκτελούμε το script `src/shell_scripts/download_datasets.sh` μόνο στον `master`. Μπορούμε προαιρετικά να δώσουμε ως argument στο script ένα μονοπάτι στο οποίο θα τοποθετήσει τα datasets. Το default είναι `~/datasets`.
6. Εκτελούμε το script `src/shell_scripts/copy_datasets.sh` μόνο στον `master`. Μπορούμε προαιρετικά να δώσουμε ως argument στο script το μονοπάτι στο οποίο βρίσκονται τα datasets. Το default είναι `~/datasets`. Τα μηχανήματα είναι πλέον έτοιμα να τρέξουν τα δικά μας queries.

## Πληροφορίες για τον κώδικα (`src/`)
* Τα αρχεία κώδικα είναι χωρισμένα σε φακέλους ανάλογα με το ποιό τμήμα του κώδικα υλοποιούν και έχουν ως όνομα το Spark API με το οποίο είναι υλοποιημένα.
* Όλα τα αρχεία (πέρα από τα shell scripts) δέχονται τα εξής arguments:
  * `-e/--explain`: εκτύπωση και του πλάνου εκτέλεσης μαζί με το output του query (στο stdout)
  * `-E/--explain-only`: εκτύπωση μόνο του πλάνου εκτέλεσης (στο stdout)
  * `-o/--output <filename>`: redirection του stdout στο δοσμένο filename
  * `-s/--join-strategy {broadcast,merge,shuffle_hash,shuffle_replicate_nl}`: χρήση της δοσμένης στρατηγικής σε κάθε join του query (μόνο τα queries 3 και 4 έχουν joins)
* `preprocessing/`: ανάγνωση των `Crime_Data_from_2010_to_2019.csv`, `Crime_Data_from_2020_to_Present.csv`, συνένωση και αποθήκευση τους στο `Crime_Data_from_2010_to_Present.csv` (από/πρός το HDFS)
* `query_{1,2,3,4}/`: υλοποίηση του αντίστοιχου query
* `common.py`: μερικές συναρτήσεις που χρησιμοποιούνται από όλα τα queries
* `paths.py`: μεταβλητές που περιέχουν τα μονοπάτια όλων των datasets στο HDFS
* `schemas.py`: μεταβλητές που περιέχουν τα schemas όλων των datasets
