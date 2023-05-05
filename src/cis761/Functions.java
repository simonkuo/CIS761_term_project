package cis761;
import java.util.Properties;
import java.util.Map.Entry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Functions {
	private static Properties configProps = new Properties();

	private static String MySqlServerDriver;
	private static String MySqlServerUrl;
    private static String MySqlServerUser;
	private static String MySqlServerPassword;


	// DB Connection
	private Connection _mySqlDB;

	// Canned queries

	private String _search_sql = "SELECT m.*, g.name as genre, l.name Language, c.name country, y.name company FROM movies m "
	+ "left join movieGenres mg on m.id = mg.movieID join genres g on g.id = mg.genreID "
	+ "left join movieLanguages ml on m.id = ml.movieID join languages l on l.languageCode = ml.languageCode "
	+ "left join movieCountry mc on m.id = mc.movieID join country c on c.code = mc.countryCode "
	+ "left join movieCompany my on m.id = my.movieID join company y on y.id = my.companyID ";
	private PreparedStatement _search_statement;

	/* uncomment, and edit, after your create your own user database */
	private String _user_login_sql = "SELECT * FROM users WHERE email = ? and password = ?";
	private PreparedStatement _user_login_statement;


	public Functions() {
	}

    /**********************************************************/
    /* Connection to MySQL database */

	public void openConnections() throws Exception {
        
        /* open connections to TWO databases: movie and  user databases */
        
		configProps.load(new FileInputStream("dbconn.config"));
        
		MySqlServerDriver    = configProps.getProperty("MySqlServerDriver");
		MySqlServerUrl 	   = configProps.getProperty("MySqlServerUrl");
		MySqlServerUser 	   = configProps.getProperty("MySqlServerUser");
		MySqlServerPassword  = configProps.getProperty("MySqlServerPassword");
        
                              
		/* load jdbc driver for MySQL */
		Class.forName(MySqlServerDriver).getDeclaredConstructor().newInstance();

		/* open a connection to your mySQL database that contains the movie database */
		_mySqlDB = DriverManager.getConnection(MySqlServerUrl, // database
				MySqlServerUser, // user
				MySqlServerPassword); // password
	
	}

	public void closeConnections() throws Exception {
		_mySqlDB.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		
		
		
		
		
	}


    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public User transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		/* Uncomment after you create your own users database */
		
		User user = null;

		_user_login_statement = _mySqlDB.prepareStatement(_user_login_sql);
		_user_login_statement.setString(1,name);
		_user_login_statement.setString(2,password);
	    ResultSet results = _user_login_statement.executeQuery();
		if(results != null){
			user = new User();
	    	user.setId(results.getInt(1));
			user.setName(results.getString(2));
			user.setStatus(results.getString(3));
		} 
		results.close();
		return user;
		 
	}



    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(String option, String keyword)
			throws Exception {
		ResultSet movie_set = null;
		

		if(option.equals("movie")){
			StringBuilder query = new StringBuilder(_search_sql);
			query.append("WHERE m.title like ? ORDER BY m.id");
			_search_statement = _mySqlDB.prepareStatement(_search_sql);
		}else{
			StringBuilder query = new StringBuilder(_search_sql);
			query.append("WHERE mg.name like ? ORDER BY m.id");
			_search_statement = _mySqlDB.prepareStatement(_search_sql);
		}

		_search_statement.setString(1, '%' + keyword + '%');
		movie_set = _search_statement.executeQuery();


		while (movie_set.next()) {
			String movie_id = movie_set.getString(1);
			String genre = movie_set.getString(8) != null ? movie_set.getString(8) : "UNKNOWN";
			String language = movie_set.getString(9) != null ? movie_set.getString(9) : "UNKNOWN";
			String productionCountry = movie_set.getString(10) != null ? movie_set.getString(10) : "UNKNOWN";
			String productionCompany = movie_set.getString(11) != null ? movie_set.getString(11) : "UNKNOWN";

			System.out.println("ID: " + movie_id + " NAME: " + movie_set.getString(2));
			System.out.println("BUDGET: " + movie_set.getString(3) + " OVERVIEW: " + movie_set.getString(4));
			System.out.println("RELEASE DATE: " + movie_set.getString(5) + " REVENUE: " + movie_set.getString(6));
			System.out.println("RUNTIME: " + movie_set.getString(7) + " GENRE: " + genre);
			System.out.println("LANGUAGE: " + language + " PRODUCTION COMPANY: " + productionCompany);
			System.out.println("PRODUCTION country: " + productionCountry);
			System.out.println("----------------------------------------------------");
		}
		System.out.println();
	}


	

}
