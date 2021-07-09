package com.mycompany.wuzzuf_spring_web_service.spring;

/**
 *
 * @author omar
 */
import org.apache.commons.io.FileUtils;
import org.knowm.xchart.CategoryChart;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.BitmapEncoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.apache.spark.api.java.function.ForeachFunction ;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors ;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

//@RequestMapping("spark-context")
@Controller
public class SparkController {
    @Autowired
    private SparkSession sparkSession;
    
    public Dataset<Row> dataset = null;
    public Dataset<Row>Company_dataset = null;
    public Dataset <Row> Title_dataset = null;
    public Dataset <Row> Location_dataset = null;
    public Dataset <Row> skills_dataset = null;
      static ArrayList<String> final_list = new ArrayList<String>();
    static Map<String, Integer> mapp = new HashMap();
    
    
    public void read_data() {
        if (dataset != null)
            return;
        dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
    }
    
   public ResponseEntity<String> convert_to_json(Dataset<Row> data) {

        Dataset<String> df = data.toJSON();
        String s = df.showString(10000, 30000, false);
        String[] _ = s.split("\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < _.length; i += 2)
            try {
                sb.append(_[i].split("\\|")[1] + "<br>");
            } catch (Exception e) {

            }
        return ResponseEntity.ok(sb.toString());
    }
    @RequestMapping("read-data")
    public ResponseEntity<String> getRowCount() {
       dataset = sparkSession.read().option("header", "true").csv("Wuzzuf_Jobs.csv");
     
        return convert_to_json(dataset) ;
       
    }
    
    // Print Schema , First 5 rows of the datase tand Summary
    Dataset<Row> get_schema(){
        
        read_data();
        Dataset<Row> schema = dataset.describe();
        return schema;
     }
    @RequestMapping("schema")
    public ResponseEntity<String> show_schema(){
        Dataset<Row>schema = get_schema();
        return convert_to_json(schema);
    }
    /////////////////////////////////////////////////////////////////////////////////////////
    
       // Clean the data (null , duplications)
 
    Dataset<Row> clean_data(){
        
             read_data();
             dataset = dataset.distinct()
                .filter(col("Title").notEqual(""))
                .filter(col("Company").notEqual(""))
                .filter(col("Location").notEqual(""))
                .filter(col("Type").notEqual(""))
                .filter(col("Level").notEqual(""))
                .filter(col("YearsExp").notEqual(""))
                .filter(col("Country").notEqual(""))
                .filter(col("Skills").notEqual(""));
             return dataset;
     }
    
     @RequestMapping("cleanData")
    public ResponseEntity<String> show_clean_data(){
        read_data();
        dataset = clean_data();
        return convert_to_json(dataset);
    }
    ///////////////////////////////////////////////////////////////////////////////////////////
     /* Count the jobs for each company 
        and display that in order (What are the most demanding companies for jobs?)*/
    
    Dataset<Row> count_jobs(){
        
        read_data();
       RelationalGroupedDataset dataset_grouped_by_company = dataset.groupBy("Company");
       Company_dataset = dataset_grouped_by_company.count()
                                                           .sort(desc("count"));
      return Company_dataset;
      
     }
    @RequestMapping("countJobs")
    public ResponseEntity<String> show_count_jobs(){
        read_data();
        Company_dataset = count_jobs();
        String html = String.format("<h3>Count jobs %s</h3>",Company_dataset)+
                Company_dataset.showString(10, 0, true);
        return ResponseEntity.ok(html);
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Show step 4 in a pie chart 
    
    
     @Autowired
 public PieChart display_pie_Company() throws IOException {
     
   Company_dataset= count_jobs();
  List <String> company_names = Company_dataset.select("Company").limit(5).as(Encoders.STRING())                                                                 .collectAsList();
   List <Long> company_num = Company_dataset.select("Count").limit(5).as(Encoders.LONG())
                                                                      .collectAsList();
     
        PieChart chart = new PieChartBuilder().width(700).height(700).title("Jobs Per Companies").build();
        for(int i = 0 ; i<company_names.size() ; i++){
            chart.addSeries(company_names.get(i) , company_num.get(i));
        }
        
       // new SwingWrapper (chart).displayChart ();
        // save chart as an image
        
        BitmapEncoder.saveBitmap(chart, "./public/PieChart", BitmapEncoder.BitmapFormat.PNG);
        return chart;
        
    }

    @RequestMapping("/pieCompany")
    public ResponseEntity<String> display_image() {
        return ResponseEntity.ok("<img src=\"PieChart.png\" />");
    } 
  ///////////////////////////////////////////////////////////////////////////////////  
     // Find out What are it the most popular job titles? 
      
    Dataset<Row> count_titles(){
        
        read_data();
        RelationalGroupedDataset dataset_grouped_by_title = dataset.groupBy("Title");
       Dataset <Row> Title_dataset = dataset_grouped_by_title.count().sort(desc("count"));
      
      return Title_dataset;
      
     }
    @RequestMapping("countTitles")
    public ResponseEntity<String> show_count_titles(){
        read_data();
        Title_dataset = count_titles();
        String html = String.format("<h3>Count titles %s</h3>",Title_dataset)+
                Title_dataset.showString(10, 0, true);
        return ResponseEntity.ok(html);
    }
    
       ////////////////////////////////////////////////////////////////////////////////////////
      
      // Show step 6 in bar chart 
      
         @Autowired
 public CategoryChart display_bar_titles() throws IOException {
     
   Title_dataset= count_titles();
   List <String> title_names = Title_dataset.select("Title").limit(5)
                                                                .as(Encoders.STRING())
                                                                .collectAsList();
   List <Long> title_num = Title_dataset.select("Count").limit(5)
                                                            .as(Encoders.LONG())
                                                            .collectAsList();
        CategoryChart chart = new CategoryChartBuilder().width(800).height(800).title("Jobs Per Titles")
                                                                             .xAxisTitle("Title Name")
                                                                             .yAxisTitle("count")
                                                                              .build();
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        chart.getStyler ().setStacked(true);
        chart.addSeries("Jobs Per Titles", title_names, title_num);
      //  new SwingWrapper (chart).displayChart ();
         BitmapEncoder.saveBitmap(chart, "public/barchart_title", BitmapEncoder.BitmapFormat.PNG);
        return chart;
    }

    @RequestMapping("/barTitle")
    public ResponseEntity<String> display_bar_title() {
        return ResponseEntity.ok("<img src=\"barchart_title.png\" />");
    }
    
     //////////////////////////////////////////////////////////////////////////////////////
       
        // Find out the most popular areas?
    
      Dataset<Row> count_locations(){
        
        read_data();
        RelationalGroupedDataset dataset_grouped_by_location = dataset.groupBy("Location");
        Location_dataset = dataset_grouped_by_location.count().sort(desc("count"));
      return Location_dataset;
      
     }
    @RequestMapping("countLocations")
    public ResponseEntity<String> show_count_locations(){
        read_data();
        Location_dataset = count_locations();
        String html = String.format("<h3>Count locations %s</h3>",Location_dataset)+
                Location_dataset.showString(10, 0, true);
        return ResponseEntity.ok(html);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////
       
        // Show step 8 in bar chart 
       
            @Autowired
 public CategoryChart display_bar_locations() throws IOException {
     
    Location_dataset = count_locations();
   List <String> area_names = Location_dataset.select("Location").limit(5).as(Encoders.STRING()).collectAsList();
   List <Long> area_num = Location_dataset.select("count").limit(5).as(Encoders.LONG()).collectAsList();
        
        CategoryChart chart = new CategoryChartBuilder().width(800).height(800).title("Jobs Per locations")
                                                                             .xAxisTitle("Location Name")
                                                                             .yAxisTitle("count")
                                                                              .build();
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        chart.getStyler ().setStacked(true);
        chart.addSeries("Jobs Per Areas", area_names, area_num);
      //  new SwingWrapper (chart).displayChart ();
         BitmapEncoder.saveBitmap(chart, "public/barchart_location", BitmapEncoder.BitmapFormat.PNG);
        return chart;
    }

    @RequestMapping("/barLocation")
    public ResponseEntity<String> display_bar_location() {
        return ResponseEntity.ok("<img src=\"barchart_location.png\" />");
    }
     ///////////////////////////////////////////////////////////////////////////////////////////
        
        /*
        Print skills one by one and 
        how many each repeated and order the output to find out the most important skills required?
        */
    
    Dataset<Row> count_skills(){
        
        read_data();
      RelationalGroupedDataset dataset_grouped_by_skills = dataset.groupBy("Skills");
      skills_dataset = dataset_grouped_by_skills.count().sort(desc("count"));
      return skills_dataset;
      
     }
    @RequestMapping("countSkills")
    public ResponseEntity<String> show_count_skills(){
        read_data();
        skills_dataset = count_skills();
        String html = String.format("<h3>Count skills %s</h3>",skills_dataset)+
                skills_dataset.showString(10, 0, true);
        return ResponseEntity.ok(html);
    }
    
     ////////////////////////////////////////////////////////////////////////////////////
        
        // Show step 9 in bar chart
    
    
    @Autowired
 public CategoryChart display_bar_skill() throws IOException {
     
    skills_dataset = count_skills();
   List <String> skills_Name = skills_dataset.select("Skills").limit(10).as(Encoders.STRING()).collectAsList();
   List <Long> skills_num = skills_dataset.select("count").limit(10).as(Encoders.LONG()).collectAsList();
        
        CategoryChart chart = new CategoryChartBuilder().width(800).height(800).title("Jobs Per skills")
                                                                             .xAxisTitle("skill Name")
                                                                             .yAxisTitle("count")
                                                                              .build();
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        chart.getStyler ().setStacked(true);
        chart.addSeries("Jobs Per skills", skills_Name, skills_num);
      //  new SwingWrapper (chart).displayChart ();
         BitmapEncoder.saveBitmap(chart, "public/barchart_skills", BitmapEncoder.BitmapFormat.PNG);
        return chart;
    }

    @RequestMapping("/barSkills")
    public ResponseEntity<String> display_bar_skills() {
        return ResponseEntity.ok("<img src=\"barchart_skills.png\" />");
    }
    
    //////////////////////////////////////////////////////////////////////////////
    
    // Factorize the YearsExp feature and convert it to numbers in new col. (Bounce )
   @RequestMapping("factorizeYrsOfExp")
    public ResponseEntity<String> factorize() throws JsonProcessingException {
        read_data();
       RelationalGroupedDataset dataset_grouped_by_YearsExp = dataset.groupBy("YearsExp");
       Dataset <Row> Years_Exp_dataset = dataset_grouped_by_YearsExp.count().sort(desc("count"));
       
      
        List <String> Years_EXP_names = Years_Exp_dataset.select("YearsExp").as(Encoders.STRING()).collectAsList();

       
        Dataset <Row> [] YearsExpList = new Dataset [Years_EXP_names.size()];
        for (int i = 0; i < Years_EXP_names.size();i++)
        {
            Dataset <Row> current = dataset.filter(col("YearsExp").contains(Years_EXP_names.get(i)));
            YearsExpList[i] = current;
        }
        
        String html = String.format("<h3>factorizeYrsOfExp %s</h3>",YearsExpList)+
                YearsExpList[0].showString(1, 0, true);
        
        
        
        return ResponseEntity.ok(html);
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
}