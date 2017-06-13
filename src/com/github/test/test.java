package com.github.test;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

import ring.middleware.file_info__init;
import rx.functions.Func2;

import backtype.storm.tuple.Values;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

public class test {
	private static RTree<String, Point> tree ;
	private static File file;
	public static void main(String[] args) throws Exception {
		file = new File(args[0]);
		tree= RTree.create().maxChildren(5).create();
		java.util.List<String> lines = FileUtils.readLines(file,"UTF-8");
		long begin = System.currentTimeMillis();
		int k=0;
		int m=0;
		for (String line : lines) {
			if(line.split("\t")[0].equals("newpoint"))
			{
				m++;
				tree = tree.add(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
			}
			if(line.split("\t")[0].equals("point"))
			{
				m++;
				tree = tree.delete(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
				tree = tree.add(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[4]), Double.parseDouble(line.split("\t")[5])));
			}
			if(line.split("\t")[0].equals("disappearpoint"))
			{
				m++;
				tree = tree.delete(line.split("\t")[1], Geometries.point(Double.parseDouble(line.split("\t")[2]), Double.parseDouble(line.split("\t")[3])));
			}
			k++;
			if(k!=m)
			{
				System.out.print("fuck");
			}
			System.out.print(k+"\n");
		}
		long end = System.currentTimeMillis();
		double ratio = (end-begin)/k;
		System.out.print(ratio);
		//		tree = tree.delete("1", Geometries.point(10,20));
		//		Func2<? super Point, ? super Point, Double> distance = new Func2<Point, Point, Double>() {
		//
		//			@Override
		//			public Double call(Point arg0, Point arg1) {
		//				
		//				
		//				// TODO Auto-generated method stub
		//				return Math.sqrt(Math.pow(arg0.x()-arg1.x(),2)+Math.pow(arg0.y()-arg1.y(),2));
		//			}
		//		};
		//		Iterable<Entry<String, Point>> it = tree.search(Geometries.point(98, 125), 10, distance)
		//				.toBlocking().toIterable();
		//		 Iterator<Entry<String, Point>> iter = it.iterator();
		//		 Boolean bool = tree.isEmpty();		 
		//		while(iter.hasNext())
		//		{
		//			Entry<String, Point> a = iter.next();			
		//			Point b = a.geometry();
		//			System.out.print(b.x()+",");
		//			System.out.print(b.y()+",");
		//			System.out.print(a.value()+"\n");
		//		}

	}


}
