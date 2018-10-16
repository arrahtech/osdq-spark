package org.arrah.dq.spark.core.dataprocess;

import java.util.HashMap;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;



/* This class will be used for creating
 * unique identifier across different data
 * source
 * 
 */
public class CosineDistance implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CosineDistance() {

	}

	public static double dot(Vector vectorOne,Vector vectorTwo){
		double[] doubleVectorOne = vectorOne.toArray();
		double[] doubleVectorTwo = vectorTwo.toArray();
		if(doubleVectorOne.length!=doubleVectorTwo.length) throw new RuntimeException("Lengths are not equal");
		double sum = 0.0;
		for(int i=0;i<doubleVectorOne.length; i++){
			sum = sum + doubleVectorOne[i]*doubleVectorTwo[i];
		}
		return sum;
	}

	public static double cosineSimilarity(Vector vectorOne,Vector vectorTwo){
		double normOne = Vectors.norm(vectorOne, 2.0);
		double normTwo = Vectors.norm(vectorTwo, 2.0);
		double denominator = normOne*normTwo;
		double numarator = dot(vectorOne,vectorTwo);
		double cosineValue = numarator/denominator;
		return cosineValue;
	}

	public static double[][] vectoriseString (String first, String second) {
		double [][] newvec = new double[2][];
		if (first == null || second  == null || "".equals(first) || "".equals(second)) // nothing to compare
			return newvec;
		
		HashMap<Character,Integer> firstMap = uniqueCharCount(first);
		HashMap<Character,Integer> secondMap = uniqueCharCount(second);
		
		Object[] fullkeySet = getUnion(firstMap.keySet().toArray(),secondMap.keySet().toArray());
		
		newvec[0] = new double[fullkeySet.length];
		newvec[1] = new double[fullkeySet.length];
		
		for (int i=0; i  < fullkeySet.length; i++ ){
			char c = (Character) fullkeySet[i];
			
			if (firstMap.containsKey(c) == true) 
				newvec[0][i] = firstMap.get(c);
			else
				newvec[0][i] = 0.0D;
			
			if (secondMap.containsKey(c) == true) 
				newvec[1][i] = secondMap.get(c);
			else
				newvec[1][i] = 0.0D;
		}
		
		return newvec;
	
	}
	
	public static HashMap<Character,Integer> uniqueCharCount (String showUnique) {
		if (showUnique == null || "".equals(showUnique) ) // nothing to compare
			return null;
		HashMap<Character,Integer> uniqMap = new HashMap<Character,Integer>();
		for (int i=0; i < showUnique.length(); i++  ) {
			char c = showUnique.charAt(i);
			if (uniqMap.containsKey(c) == false) // Not found
				uniqMap.put(c, 1);
			else
				uniqMap.put(c, uniqMap.get(c)+1); // increase the counter
		}
		return uniqMap;
	}
	
	public static Object[] getUnion (Object[] setA, Object[] setB) {
		java.util.Vector<Object> resultSet = null;
		Object[] smallSet,bigSet;
		
		if ( setA.length > setB.length){
			smallSet = setB;
			bigSet = setA;
		} else {
			smallSet = setA;
			bigSet = setB;
		}
		
		int ilen = smallSet.length;
		if (ilen == 0 ) {
			return bigSet;
		}
		
		resultSet = new java.util.Vector<Object>();
		
		for (int i=0 ; i < ilen; i++ ) {
			Object o = smallSet[i];
			if (resultSet.indexOf(o) == -1 ) // it is not found in resulting set
				resultSet.add(o);
		}
		int ilenB = bigSet.length;
		for (int i=0 ; i < ilenB; i++ ) {
			Object o = bigSet[i];
			if (resultSet.indexOf(o) == -1 ) // it is not found in resulting set
				resultSet.add(o);
		}
		return resultSet.toArray();
	}
	
	public static double showCosineDistance(String a, String b) {
		double val = 0.0D;
		double[][] newvec = vectoriseString(a,b);
		Vector a0 =  Vectors.dense(newvec[0]);
		Vector a1 =  Vectors.dense(newvec[1]);
		val = cosineSimilarity(a0,a1);;
		return val;	
	}
	
	public static void main(String[] args)  {
		String a = "vviek NBC";
		String b =  "vivek";
		
		
		double[][] newvec = vectoriseString(a,b);
		
		System.out.println("First String:" + a);
		for (int i=0; i < newvec[0].length; i++ )
			System.out.print(newvec[0][i]);
		System.out.println("");
		
		System.out.println("Second String:" + b);
		for (int i=0; i < newvec[1].length; i++ )
		 System.out.print(newvec[1][i]);
		System.out.println("");
		
		Vector a0 =  Vectors.dense(newvec[0]);
		Vector a1 =  Vectors.dense(newvec[1]);
		
		double d = cosineSimilarity(a0,a1);
		
		System.out.println("Cos0 is:" + d);
		
	}
	
}
