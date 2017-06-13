package index;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import backtype.storm.command.list;

import com.esri.core.geometry.GeometryEngine;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Shape;

import model.Point_Mdl;

public class LuceneIndex {
	private JtsSpatialContext ctx;
	private  SpatialStrategy strategy;
	private int maxLevels;
	private SpatialPrefixTree grid;
	public LuceneIndex() {
		super();
		this.analyzer = new StandardAnalyzer();
		 this.maxLevels = 11;//results in sub-meter precision for geohash
		this.ctx = JtsSpatialContext.GEO;
		 grid = new GeohashPrefixTree(ctx, maxLevels);
		 this.strategy = new RecursivePrefixTreeStrategy(grid, "myGeoField");
	}
	
	private Analyzer analyzer;
	
	
	
	
	private void initialDirectory(Directory indexDirectory) throws IOException
	{
		if(indexDirectory ==  null)
		{
			indexDirectory = new RAMDirectory();
		}
	}

	public void buildIndex(Directory indexDirectory,Point_Mdl point) throws Exception
	{			
		initialDirectory(indexDirectory);
//		HashMap<String, String>a = new HashMap<String,String>();
//		a.put("geo", "false");
//		a.put("distCalculator", "cartesian^2");
//		a.put("worldBounds", "ENVELOPE(-100, 75, 200, 0)");
		indexPoints(point,indexDirectory);
	}
	
	public void deletepoint(String ID,Directory indexDirectory) throws IOException
	{
		 IndexWriterConfig iwConfig =new IndexWriterConfig(analyzer);
			IndexWriter write = new IndexWriter(indexDirectory, iwConfig);
			IndexReader indexReader = DirectoryReader.open(indexDirectory);
			IndexSearcher indexSearcher = new IndexSearcher(indexReader);
			int maxLevels = 11;//results in sub-meter precision for geohas	;
			write.deleteDocuments(new Term("id", ID));
	}
	
	
	private void indexPoints(Point_Mdl point,Directory indexDirectory) throws Exception {

		//Spatial4j is x-y order for arguments
		int maxLevels = 11;//results in sub-meter precision for geohas	;
		int Id = point.get_id();
		//		Date time = point.get_date();
		//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		//		String strTime = sdf.format(time);
		 IndexWriterConfig iwConfig =new IndexWriterConfig(analyzer);
		IndexWriter write = new IndexWriter(indexDirectory, iwConfig);
		IndexReader indexReader = DirectoryReader.open(indexDirectory);
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		String wkt = GeometryEngine.geometryToWkt(point.get_point(), 0);
		if(!SearchDocment(point.get_id(),indexSearcher))
		{
			write.addDocument(newSampleDocument(
					Id, ctx.readShapeFromWkt(wkt))) ;
		}
		else
		{
			String ID = String.valueOf(point.get_id()) ;
			write.updateDocument(new Term("id", ID),newSampleDocument(
					Id, ctx.readShapeFromWkt(wkt)));
		}
		write.close();
		indexReader.close();
	}
	
	//´ýÐÞ¸Ä
	private boolean SearchDocment(int ID,IndexSearcher indexSearcher)
	{
		Query q = NumericRangeQuery.newIntRange("id", ID, ID, true, true);			
		TopDocs hits;
		try {
			hits = indexSearcher.search(q, 1);			
			int docId = hits.scoreDocs[0].doc;
			if(hits.totalHits>0)
			{
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}


	private Document newSampleDocument(int id, Shape... shapes) throws IOException {
		Document doc = new Document();
		//		FieldType Ft =  new FieldType();	 
		doc.add(new IntField("id", id,  Field.Store.YES));
		//		doc.add(new StringField("date", date,  Field.Store.YES));
		//Potentially more than one shape in this field is supported by some
		// strategies; see the javadocs of the SpatialStrategy impl to see.
		for (Shape shape : shapes) {
			for (Field f : strategy.createIndexableFields(shape)) {
				doc.add(f);
			}
		}
		return doc;
	}


//	private void deletePoint(Point_Mdl point,Directory indexDirectory) throws IOException
//	{
//
//		IndexWriter write = new IndexWriter(indexDirectory, iwConfig);
//  
//
//	}

	public List<Integer> searchPoint(Directory indexDirectory,double latiude, double longtitude,double radius) throws IOException
	{
          List<Integer>a = new ArrayList<>();
		SpatialArgs args = new SpatialArgs(SpatialOperation.Contains,
				ctx.makeCircle(longtitude, latiude,radius));
		Filter filter = strategy.makeFilter(args);
		IndexReader indexReader = DirectoryReader.open(indexDirectory);
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		TopDocs hits = indexSearcher.search(filter, 100);
		List<Point_Mdl>point = new ArrayList<>();
		for (int i = 0; i < hits.totalHits; i++)
		{
			final int docID = hits.scoreDocs[i].doc;
			final Document doc = indexSearcher.doc(docID);
			a.add(Integer.valueOf(doc.get("id")));
		}  
		return a ;
	}
}
