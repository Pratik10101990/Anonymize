package com;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.LinkedHashMap;
import java.io.File;
public class SparkAnonymize {

	static int mid_age = 0;
	static LinkedHashMap<String,ArrayList<String>> anonymize = new LinkedHashMap<String,ArrayList<String>>();
	static List<String> list;
	static String columns[]={"Age","Work Class","Salary","Education","Education-Num","Marital Status","Occupation","Relationship","Race","Sex","Balance","Hours Per Week","Country"};
	
public static void run(File file) {
	JavaSparkContext context = new JavaSparkContext("local","SparkAnonymize");
	JavaRDD<String> data = context.textFile(file.getPath());
    list = data.top(1000);
	ArrayList<String> l1 = new ArrayList<String>();
	ArrayList<String> l2 = new ArrayList<String>();
	ArrayList<String> l3 = new ArrayList<String>();
	anonymize.put("less",l1);
	anonymize.put("greater",l2);
	anonymize.put("equal",l3);
	
	if(file.getName().equals("dataset.txt")){
		for(int i=1;i<list.size();i++){
			String arr[] = list.get(i).split(",");
			mid_age = mid_age + Integer.parseInt(arr[1]);
		}
		anonimizeDataset();
	}
	if(file.getName().equals("adult.txt")){
		for(int i=0;i<list.size();i++){
			String arr[] = list.get(i).split(",");
			mid_age = mid_age + Integer.parseInt(arr[0]);
		}
		anonimizeAdult();
	}
}
public static void getAge(){
	mid_age = mid_age/list.size();
	boolean flag = true;
	while(flag){
		String str = Integer.toString(mid_age);
		if(str.endsWith("0")){
			flag = false;
			break;
		}else{
			mid_age = mid_age + 1;
		}
	}
}
public static void anonimizeDataset(){
	getAge();
	for(int i=1;i<list.size();i++){
		String arr[] = list.get(i).split(",");
		int age = Integer.parseInt(arr[1]);
		if(age < mid_age){
			String zip = arr[0].trim();
			String race = arr[2].trim();
			String diagnosis = arr[3].trim();
			zip = zip.substring(0,zip.length()-1)+"*";
			String data = zip+",<"+mid_age+",*,"+diagnosis;
			anonymize.get("less").add(data);
		}
		if(age > mid_age){
			String zip = arr[0].trim();
			String race = arr[2].trim();
			String diagnosis = arr[3].trim();
			zip = zip.substring(0,zip.length()-1)+"*";
			String data = zip+",>"+mid_age+",*,"+diagnosis;
			anonymize.get("greater").add(data);
		}
		if(age == mid_age){
			String zip = arr[0].trim();
			String race = arr[2].trim();
			String diagnosis = arr[3].trim();
			zip = zip.substring(0,zip.length()-1)+"*";
			String data = zip+",="+mid_age+",*,"+diagnosis;
			anonymize.get("equal").add(data);
		}
	}
}
public static void anonimizeAdult(){
	getAge();
	for(int i=0;i<list.size();i++){
		String arr[] = list.get(i).split(",");
		int age = Integer.parseInt(arr[0]);
		if(age < mid_age){
			String zip = arr[2].trim();
			zip = zip.substring(0,zip.length()-1)+"*";
			String education = arr[3].trim();
			String gender = arr[9].trim();
			String occupation = arr[6].trim();
			String race = arr[8].trim();
			String relationship = arr[7].trim();
			String status = arr[5].trim();
			String country = arr[13].trim();
			String workclass = arr[1].trim();
			String data = zip+","+education+",<"+mid_age+","+gender+","+occupation+",*,"+relationship+","+status+",*,"+workclass;
			anonymize.get("less").add(data);
		}
		if(age > mid_age){
			String zip = arr[2].trim();
			zip = zip.substring(0,zip.length()-1)+"*";
			String education = arr[3].trim();
			String gender = arr[9].trim();
			String occupation = arr[6].trim();
			String race = arr[8].trim();
			String relationship = arr[7].trim();
			String status = arr[5].trim();
			String country = arr[13].trim();
			String workclass = arr[1].trim();
			String data = zip+","+education+",>"+mid_age+","+gender+","+occupation+",*,"+relationship+","+status+",*,"+workclass;
			anonymize.get("greater").add(data);
		}
		if(age == mid_age){
			String zip = arr[2].trim();
			zip = zip.substring(0,zip.length()-1)+"*";
			String education = arr[3].trim();
			String gender = arr[9].trim();
			String occupation = arr[6].trim();
			String race = arr[8].trim();
			String relationship = arr[7].trim();
			String status = arr[5].trim();
			String country = arr[13].trim();
			String workclass = arr[1].trim();
			String data = zip+","+education+","+mid_age+","+gender+","+occupation+",*,"+relationship+","+status+",*,"+workclass;
			anonymize.get("equal").add(data);
		}
	}
}

}