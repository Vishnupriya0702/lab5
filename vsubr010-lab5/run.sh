spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar count-all  Sample2.tsv
spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar code-filter Sample2.tsv 302 
spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar time-filter Sample2.tsv 302 
spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar count-by-code Sample2.tsv 
spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar sum-bytes-by-code Sample2.tsv 
spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar top-host Sample2.tsv 
spark-submit --class edu.ucr.cs.cs167.vsubr010.App --master local[2] target/vsubr010-lab5-1.0-SNAPSHOT.jar comparison Sample2.tsv 
