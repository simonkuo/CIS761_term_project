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
	
	private static String PsotgreSqlServerDriver;
	private static String PostgreSqlServerUrl;
	private static String PostgreSqlServerUser;
	private static String PostgreSqlServerPassword;
	private static String PostgreSqlServerSSL;


	// DB Connection
	private Connection _mySqlDB;
    private Connection _postgreSqlDB;

	// Canned queries

	private String _search_sql = "SELECT * FROM movie_info WHERE movie_name like ? ORDER BY movie_id";
	private PreparedStatement _search_statement;

	private String _producer_id_sql = "SELECT y.* "
					 + "FROM producer_movies x, producer_ids y "
					 + "WHERE x.movie_id = ? and x.producer_id = y.producer_id";
	private PreparedStatement _producer_id_statement;

	private String _actor_id_sql = "SELECT y.* "
					 + "FROM actor_movies x, actor_ids y "
					 + "WHERE x.movie_id = ? and x.actor_id = y.actor_id";
	private PreparedStatement _actor_id_statement;

	/* uncomment, and edit, after your create your own customer database */
	private String _customer_login_sql = "SELECT * FROM customer WHERE login = ? and password = ?";
	private PreparedStatement _customer_login_statement;

	private String _rental_check_sql = "SELECT cid FROM rental WHERE movie_id = ? and status = 'open'";
	private PreparedStatement _rental_check_statement;

	private String _check_plan_sql = "SELECT plan_id FROM plan WHERE plan_id = ?";
	private PreparedStatement _check_plan_statement;

	private String _update_customer_plan_sql = "UPDATE customer set plan_id = ? where cid = ?";
	private PreparedStatement _update_customer_plan_statement;

	private String _list_all_plan_sql = "SELECT * FROM plan";
	private PreparedStatement _list_all_plan_statement;

	private String _rent_movie_sql = "INSERT INTO rental (cid, movie_id, status, time) values (?, ?, ?, ?)";
	private PreparedStatement _rent_movie_statement;

	private String _customer_info_sql = "SELECT first_name, last_name, plan_id FROM customer WHERE cid = ?";
	private PreparedStatement _customer_info_statement;

	private String _get_max_rental_sql = "SELECT p.max_rental FROM customer c join plan p on p.plan_id = c.plan_id WHERE c.cid = ?";
	private PreparedStatement _get_max_rental_statement;

	private String _get_plan_max_rental_sql = "SELECT max_rental FROM plan where plan_id = ?";
	private PreparedStatement _get_plan_max_rental_statement;
	
	private String _get_rent_count_sql = "SELECT count(*) FROM rental where cid = ? and status = 'open'";
	private PreparedStatement _get_rent_count_statement;

	private String _get_previous_rent_count_sql = "SELECT count(*) FROM rental where cid = ? and movie_id = ?";
	private PreparedStatement _get_previous_rent_count_statement;

	private String _return_rental_sql = "UPDATE rental set status = 'closed' where cid = ? and movie_id = ? and status = 'open'";
	private PreparedStatement _return_rental_statement;


	private String _fast_search_sql = "SELECT * FROM movie_info WHERE movie_name like ? ORDER BY movie_id";
	private PreparedStatement _fast_search_statement;

	private String _fast_producer_id_sql = "SELECT m.movie_id, y.producer_name "
					 + "FROM movie_info m, producer_movies x, producer_ids y "
					 + "WHERE m.movie_name like ? and x.movie_id = m.movie_id and x.producer_id = y.producer_id order by m.movie_id";
	private PreparedStatement _fast_producer_id_statement;

	private String _fast_actor_id_sql = "SELECT m.movie_id, y.actor_name "
					 + "FROM movie_info m, actor_movies x, actor_ids y "
					 + "WHERE m.movie_name like ? and x.movie_id = m.movie_id and x.actor_id = y.actor_id order by m.movie_id";
	private PreparedStatement _fast_actor_id_statement;

	public Functions() {
	}

    /**********************************************************/
    /* Connection to MySQL database */

	public void openConnections() throws Exception {
        
        /* open connections to TWO databases: movie and  customer databases */
        
		configProps.load(new FileInputStream("dbconn.config"));
        
		MySqlServerDriver    = configProps.getProperty("MySqlServerDriver");
		MySqlServerUrl 	   = configProps.getProperty("MySqlServerUrl");
		MySqlServerUser 	   = configProps.getProperty("MySqlServerUser");
		MySqlServerPassword  = configProps.getProperty("MySqlServerPassword");
        
        PsotgreSqlServerDriver    = configProps.getProperty("PostgreSqlServerDriver");
        PostgreSqlServerUrl 	   = configProps.getProperty("PostgreSqlServerUrl");
        PostgreSqlServerUser 	   = configProps.getProperty("PostgreSqlServerUser");
        PostgreSqlServerPassword  = configProps.getProperty("PostgreSqlServerPassword");
                              
		/* load jdbc driver for MySQL */
		Class.forName(MySqlServerDriver).getDeclaredConstructor().newInstance();

		/* open a connection to your mySQL database that contains the movie database */
		_mySqlDB = DriverManager.getConnection(MySqlServerUrl, // database
				MySqlServerUser, // user
				MySqlServerPassword); // password
		
		//_postgreSqlDB = DriverManager.getConnection(PostgreSqlServerUrl);
     
        /* load jdbc driver for PostgreSQL */
        Class.forName(PsotgreSqlServerDriver).getDeclaredConstructor().newInstance();
        
        String PostgreSqlConnectionString = PostgreSqlServerUrl+"?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&user="+
        		PostgreSqlServerUser+"&password=" + PostgreSqlServerPassword;
        
        
        /* open a connection to your postgreSQL database that contains the customer database */
        _postgreSqlDB = DriverManager.getConnection(PostgreSqlConnectionString);
        		
        		//DriverManager.getConnection(PostgreSqlServerUrl, // database
                                              // PostgreSqlServerUser, // user
                                              // PostgreSqlServerPassword); // password
	
	}

	public void closeConnections() throws Exception {
		_mySqlDB.close();
       // _postgreSqlDB.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		_search_statement = _mySqlDB.prepareStatement(_search_sql);
		_producer_id_statement = _mySqlDB.prepareStatement(_producer_id_sql);
		_actor_id_statement = _mySqlDB.prepareStatement(_actor_id_sql);

		/* uncomment after you create your customers database */
		
		_customer_login_statement = _postgreSqlDB.prepareStatement(_customer_login_sql);
		
		/* add here more prepare statements for all the other queries you need */
		_rental_check_statement = _postgreSqlDB.prepareStatement(_rental_check_sql);
		_check_plan_statement = _postgreSqlDB.prepareStatement(_check_plan_sql);
		_update_customer_plan_statement = _postgreSqlDB.prepareStatement(_update_customer_plan_sql);
		_list_all_plan_statement = _postgreSqlDB.prepareStatement(_list_all_plan_sql);
		_customer_info_statement = _postgreSqlDB.prepareStatement(_customer_info_sql);
		_get_max_rental_statement = _postgreSqlDB.prepareStatement(_get_max_rental_sql);
		_get_plan_max_rental_statement = _postgreSqlDB.prepareStatement(_get_plan_max_rental_sql);
		_get_rent_count_statement = _postgreSqlDB.prepareStatement(_get_rent_count_sql);
		_get_previous_rent_count_statement = _postgreSqlDB.prepareStatement(_get_previous_rent_count_sql);
		_rent_movie_statement = _postgreSqlDB.prepareStatement(_rent_movie_sql);
		_return_rental_statement = _postgreSqlDB.prepareStatement(_return_rental_sql);
		_fast_search_statement = _mySqlDB.prepareStatement(_fast_search_sql);
		_fast_producer_id_statement = _mySqlDB.prepareStatement(_fast_producer_id_sql);
		_fast_actor_id_statement = _mySqlDB.prepareStatement(_fast_actor_id_sql);
		
	}


    /**********************************************************/
    /* suggested helper functions  */

	public boolean helper_compute_is_plan_switchable(int cid, int plan_id) throws Exception {
		/* how many movies can she/he still rent ? */
		/* you have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		   int maxRentalNum = 0;
		   int rentCount = 0;
		   _get_plan_max_rental_statement.clearParameters();
		   _get_plan_max_rental_statement.setInt(1, plan_id);
		   ResultSet matRentalSet = _get_plan_max_rental_statement.executeQuery();
		   if (matRentalSet.next()) maxRentalNum = matRentalSet.getInt(1);
		   matRentalSet.close();

		   _get_rent_count_statement.clearParameters();
		   _get_rent_count_statement.setInt(1, cid);
		   ResultSet rentCountSet = _get_rent_count_statement.executeQuery();
		   if (rentCountSet.next()) rentCount = rentCountSet.getInt(1);
		   rentCountSet.close();
		return maxRentalNum >= rentCount;
	}

	public int helper_compute_remaining_rentals(int cid) throws Exception {
		/* how many movies can she/he still rent ? */
		/* you have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		   int maxRentalNum = 0;
		   int rentCount = 0;
		   _get_max_rental_statement.clearParameters();
		   _get_max_rental_statement.setInt(1, cid);
		   ResultSet matRentalSet = _get_max_rental_statement.executeQuery();
		   if (matRentalSet.next()) maxRentalNum = matRentalSet.getInt(1);
		   matRentalSet.close();

		   _get_rent_count_statement.clearParameters();
		   _get_rent_count_statement.setInt(1, cid);
		   ResultSet rentCountSet = _get_rent_count_statement.executeQuery();
		   if (rentCountSet.next()) rentCount = rentCountSet.getInt(1);
		   rentCountSet.close();
		return maxRentalNum - rentCount;
	}

	public String helper_compute_customer_name(int cid) throws Exception {
		/* you find  the name of the current customer */
		_customer_info_statement.clearParameters();
		_customer_info_statement.setInt(1, cid);
		ResultSet customer_info = _customer_info_statement.executeQuery();
		if (customer_info.next()){ 
			String name = customer_info.getString("first_name") 
			+ " " 
			+ customer_info.getString("last_name");
			customer_info.close();
			return name;
		}
		return null;

	}

	public boolean helper_check_plan(int plan_id) throws Exception {
		/* is plan_id a valid plan id?  you have to figure out */
		_check_plan_statement.clearParameters();
		_check_plan_statement.setInt(1, plan_id);
		ResultSet plan_set = _check_plan_statement.executeQuery();
	    if (plan_set.next()) {
			plan_set.close();
			return true;
		}
		return false;
	}

	public boolean helper_check_movie(String movie_id) throws Exception {
		/* is movie_id a valid movie id? you have to figure out  */
		return true;
	}

	private int helper_who_has_this_movie(String movie_id) throws Exception {
		/* find the customer id (cid) of whoever currently rents the movie movie_id; return -1 if none */
		int cid;
		_rental_check_statement.clearParameters();
		_rental_check_statement.setString(1, movie_id);
		ResultSet cid_set = _rental_check_statement.executeQuery();
	    if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		cid_set.close();
		return cid;
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		/* Uncomment after you create your own customers database */
		
		int cid;

		_customer_login_statement.clearParameters();
		_customer_login_statement.setString(1,name);
		_customer_login_statement.setString(2,password);
	    ResultSet cid_set = _customer_login_statement.executeQuery();
	    if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		cid_set.close();
		return(cid);
		 
	}

	public void transaction_personal_data(int cid) throws Exception {
		/* println the customer's personal data: name and number of remaining rentals */
		String name = this.helper_compute_customer_name(cid);
		int remainingRental = this.helper_compute_remaining_rentals(cid);
		System.out.println(name + " has " + remainingRental + " remaining rentals");
	}


    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_name)
			throws Exception {
		/* searches for movies with matching names: SELECT * FROM movie WHERE movie_name LIKE name */
		/* prints the movies, producers, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */
		/* set the first (and single) '?' parameter */
		_search_statement.clearParameters();
		_search_statement.setString(1, '%' + movie_name + '%');

		ResultSet movie_set = _search_statement.executeQuery();
		while (movie_set.next()) {
			String movie_id = movie_set.getString(1);
			System.out.println("ID: " + movie_id + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3) + " RATING: "
					+ movie_set.getString(4));
			/* do a dependent join with producer */
			_producer_id_statement.clearParameters();
			_producer_id_statement.setString(1, movie_id);
			ResultSet producer_set = _producer_id_statement.executeQuery();
			while (producer_set.next()) {
				System.out.println("\t\tProducer name: " + producer_set.getString(2));
			}
			producer_set.close();
			/* now you need to retrieve the actors, in the same manner */
			_actor_id_statement.clearParameters();
			_actor_id_statement.setString(1, movie_id);
			ResultSet actor_set = _actor_id_statement.executeQuery();
			while (actor_set.next()) {
				System.out.println("\t\tActor name: " + actor_set.getString(2));
			}
			actor_set.close();
			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			int renterCid = this.helper_who_has_this_movie(movie_id);
			if(renterCid == cid){
				System.out.println("\t\tStatus: YOU HAVE IT");
			}else if(renterCid > 0){
				System.out.println("\t\tStatus: UNAVAILABLE");
			}else{
				System.out.println("\t\tStatus: AVAILABLE");
			}
		}
		System.out.println();
	}

	public void transaction_choose_plan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
	    /* remember to enforce consistency ! */
		if(this.helper_compute_is_plan_switchable(cid, pid)){
			try{
				_postgreSqlDB.setAutoCommit(false);
				_update_customer_plan_statement.clearParameters();
				_update_customer_plan_statement.setInt(1, pid);
				_update_customer_plan_statement.setInt(2, cid);
				_update_customer_plan_statement.executeUpdate();
				_postgreSqlDB.commit();
				System.out.println("Successfully updated plan for customer " + cid);
			}catch(Exception e){
				System.out.println("can not update plan " + e.getMessage());
				_postgreSqlDB.rollback();
			}
		}else{
			System.out.println("Can not update plan due to insufficent max retal in new plan");
		}		
	}

	public void transaction_list_plans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
		ResultSet plan_set = _list_all_plan_statement.executeQuery();
		while (plan_set.next()) {
			System.out.println("\tPlan name: " + plan_set.getString(2)
			+ " Max rental number: " + plan_set.getInt(3) 
			+ " Monthly fee: " + plan_set.getFloat(4) 
			);
		}
		plan_set.close();
	}

	public void transaction_rent(int cid, String movie_id) throws Exception {
	    /* rend the movie movie_id to the customer cid */
	    /* remember to enforce consistency ! */

		int rentedCid = this.helper_who_has_this_movie(movie_id);
		if(rentedCid > 0){
			System.out.println("This movie is currently unavailable");
		}else{
			int remainingRentalCount = this.helper_compute_remaining_rentals(cid);

			if(remainingRentalCount == 0){
				System.out.println("You don't have enough quota to rent new movies");
				System.out.println("Please return prior retals");
			}else{
				int previousRentCount;

				_get_previous_rent_count_statement.clearParameters();
				_get_previous_rent_count_statement.setInt(1, cid);
				_get_previous_rent_count_statement.setString(2, movie_id);
				ResultSet previousRentCountSet = _get_previous_rent_count_statement.executeQuery();
				if (previousRentCountSet.next()) previousRentCount = previousRentCountSet.getInt(1);
				else previousRentCount = 0;
				previousRentCountSet.close();
		
				try{
					_postgreSqlDB.setAutoCommit(false);
					_rent_movie_statement.clearParameters();
					_rent_movie_statement.setInt(1, cid);
					_rent_movie_statement.setString(2, movie_id);
					_rent_movie_statement.setString(3, "open");
					_rent_movie_statement.setInt(4, previousRentCount + 1);
					_rent_movie_statement.executeUpdate();
					_postgreSqlDB.commit();
				}catch(Exception e){
					System.out.println("can not rent movie " + e.getMessage());
					_postgreSqlDB.rollback();
				}		
			}
		}		
	}

	public void transaction_return(int cid, String movie_id) throws Exception {
	    /* return the movie_id by the customer cid */
		try{
			_postgreSqlDB.setAutoCommit(false);
			_return_rental_statement.clearParameters();
			_return_rental_statement.setInt(1, cid);
			_return_rental_statement.setString(2, movie_id);
			_return_rental_statement.executeUpdate();
			_postgreSqlDB.commit();
		}catch(Exception e){
			System.out.println("can not return rental " + e.getMessage());
			_postgreSqlDB.rollback();
		}
	}

	public void transaction_fast_search(int cid, String movie_name)
			throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join producers, (c) movies join actors
		   Answers are sorted by movie_id.
		   Then merge-joins the three answer sets */

		   String movieInfoKey = "movieInfo";
		   String producerInfoKey = "producerInfo";
		   String actorInfoKey = "actorInfo";

		   Map<String, Map<String, List<String>>> mergedMovieInfo = new HashMap<>();
		   _fast_search_statement.clearParameters();
		   _fast_search_statement.setString(1, '%' + movie_name + '%');
		   ResultSet movie_set = _fast_search_statement.executeQuery();

		   _fast_producer_id_statement.clearParameters();
		   _fast_producer_id_statement.setString(1, '%' + movie_name + '%');
		   ResultSet producer_set = _fast_producer_id_statement.executeQuery();

		   _fast_actor_id_statement.clearParameters();
		   _fast_actor_id_statement.setString(1, '%' + movie_name + '%');
		   ResultSet actor_set = _fast_actor_id_statement.executeQuery();
   
		   while (movie_set.next()) {
				String movie_id = movie_set.getString(1);
				Map<String, List<String>> movieDetails = new HashMap<>();
				List<String> movieDetailList = new ArrayList<>();
				movieDetailList.add("ID: " + movie_id + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3) + " RATING: "
					+ movie_set.getString(4));
				movieDetails.put(movieInfoKey, movieDetailList);
				mergedMovieInfo.put(movie_id, movieDetails);
		   }
		   movie_set.close();

		   while (producer_set.next()) {
				String movie_id = producer_set.getString("movie_id");
				Map<String, List<String>> movieDetails = mergedMovieInfo.get(movie_id);

				if(movieDetails.containsKey(producerInfoKey)){
					movieDetails.get(producerInfoKey).add("\t\tProducer name: " + producer_set.getString("producer_name"));
				}else{
					List<String> movieDetailList = new ArrayList<>();
					movieDetailList.add("\t\tProducer name: " + producer_set.getString("producer_name"));
					movieDetails.put(producerInfoKey, movieDetailList);
				}
				mergedMovieInfo.put(movie_id, movieDetails);		   
		   }
		   producer_set.close();

		   while (actor_set.next()) {
				String movie_id = actor_set.getString("movie_id");
				Map<String, List<String>> movieDetails = mergedMovieInfo.get(movie_id);

				if(movieDetails.containsKey(actorInfoKey)){
					movieDetails.get(actorInfoKey).add("\t\tActor name: " + actor_set.getString("actor_name"));
				}else{
					List<String> movieDetailList = new ArrayList<>();
					movieDetailList.add("\t\tActor name: " + actor_set.getString("actor_name"));
					movieDetails.put(actorInfoKey, movieDetailList);
				}
				mergedMovieInfo.put(movie_id, movieDetails);			  
		   }
		   actor_set.close();

		   for (Entry<String, Map<String, List<String>>> movieDetails : mergedMovieInfo.entrySet()) {
				System.out.println(movieDetails.getValue().get(movieInfoKey).get(0));
				for(String producer : movieDetails.getValue().get(producerInfoKey)){
					System.out.println(producer);
				}
				for(String actor : movieDetails.getValue().get(actorInfoKey)){
					System.out.println(actor);
				}
		   }

		   
		   System.out.println();
	}

}
