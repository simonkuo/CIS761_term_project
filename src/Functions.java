import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
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
	
	private String select_movie_id_sql = "select id from movies where title = ?";
	private String insert_movie_sql = "insert into movies (title, budget, overview, releaseDate, revenue, runTime) values (?,?,?,?,?,?)";
	private String select_genre_sql = "select * from genres";
	private String insert_movie_genre_sql = "insert ignore into movieGenres (movieID, genreID) values (?,?)";
	private String select_language_sql = "select * from languages";
	private String insert_movie_language_sql = "insert ignore into movieLanguages (movieID,languageCode) values (?,?)";
	// private String insert_country_sql = "insert ignore into country (name) values (?)";
	// private String insert_movie_country_sql = "insert ignore into movieCountry (movieID,countryCode) values (?,?)";
	// private String insert_movie_company_sql = "insert ignore into movieCompany (movieID,companyID) values (?,?)";

	/* uncomment, and edit, after your create your own user database */
	private String _user_login_sql = "SELECT * FROM users WHERE email = ? and password = ?";



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
    /* login transaction: invoked only once, when the app is started  */
	public User transactionLogin(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		/* Uncomment after you create your own users database */
		PreparedStatement _user_login_statement;
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


	/*
	 * This method returns search results.
	 */
	public void transactionSearch(String option, String keyword)
			throws Exception {
		ResultSet movie_set = null;
		PreparedStatement _search_statement;

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

	public void transactionAddMovie(String movieName, double budget, String overview , Date releaseDate, double revenue, int runTime){

	}

	/*
	 * This method adds new movie and additonal movie details such as genres and languages.
	 */
	public void addMovie(Movies newMovie){
		PreparedStatement insertMovieStatement;
		PreparedStatement selecttMovieStatement;
		PreparedStatement insertGenreStatement;
		PreparedStatement insertLanguageStatement;
		ResultSet movieIdSet = null;
		int movieId = 0;
		try{
			try{
				_mySqlDB.setAutoCommit(false);
				insertMovieStatement = _mySqlDB.prepareStatement(this.insert_movie_sql);
				insertMovieStatement.setString(1, newMovie.getTitle());
				insertMovieStatement.setDouble(2, newMovie.getBudget());
				insertMovieStatement.setString(3, newMovie.getOverview());
				if(newMovie.getReleaseDate() != null){
					insertMovieStatement.setDate(4, java.sql.Date.valueOf(newMovie.getReleaseDate().toString()));
				}
				insertMovieStatement.setDouble(5, newMovie.getRevenue());
				insertMovieStatement.setInt(6, newMovie.getRunTime());
				insertMovieStatement.executeUpdate();
				_mySqlDB.commit();
				try{
					insertMovieStatement.close();
				}catch(Exception e){
					System.out.println("can not close PreparedStatement");
					e.printStackTrace();
				}	
			}catch(Exception e){
				_mySqlDB.rollback();
				System.out.println("can not insert new movie");
				e.printStackTrace();
			}

			try{
				selecttMovieStatement = _mySqlDB.prepareStatement(this.select_movie_id_sql);
				selecttMovieStatement.setString(1, newMovie.getTitle());
				movieIdSet = selecttMovieStatement.executeQuery();
				if(movieIdSet.next()){
					movieId = movieIdSet.getInt(1);
				}
				try{
					selecttMovieStatement.close();
				}catch(Exception e){
					System.out.println("can not close PreparedStatement");
					e.printStackTrace();
				}				
			}catch(Exception e){
				System.out.println("can not get new movie id");
				e.printStackTrace();
			}

			try{
				if(newMovie.getGenreId() > 0){
					insertGenreStatement = _mySqlDB.prepareStatement(this.insert_movie_genre_sql);
					insertGenreStatement.setInt(1, movieId);
					insertGenreStatement.setInt(2, newMovie.getGenreId());
					insertGenreStatement.executeUpdate();
					_mySqlDB.commit();
					try{
						insertGenreStatement.close();
					}catch(Exception e){
						System.out.println("can not close PreparedStatement");
						e.printStackTrace();
					}	
				}			
			}catch(Exception e){
				_mySqlDB.rollback();
				System.out.println("can not insert new genre");
				e.printStackTrace();
			}

			try{
				if(newMovie.getLanguageCode() != null){
					insertLanguageStatement = _mySqlDB.prepareStatement(this.insert_movie_language_sql);
					insertLanguageStatement.setInt(1, movieId);
					insertLanguageStatement.setString(2, newMovie.getLanguageCode());;
					insertLanguageStatement.executeUpdate();
					_mySqlDB.commit();
					try{
						insertLanguageStatement.close();
					}catch(Exception e){
						System.out.println("can not close PreparedStatement");
						e.printStackTrace();
					}	
				}				
			}catch(Exception e){
				_mySqlDB.rollback();
				System.out.println("can not insert new language");
				e.printStackTrace();
			}			
		}catch(Exception e){
			System.out.println("can not insert all movie details");
			e.printStackTrace();
		}finally{
			try{
				movieIdSet.close();
			}catch(Exception e){
				System.out.println("can not close result set");
				e.printStackTrace();
			}	
		}
	}

	/*
	 * This method returns all movie genres.
	 */
	public List<Genres> getGenres(){
		List<Genres> genres = new ArrayList<>();
		ResultSet genresSet = null;
		PreparedStatement selectGenresStatement;

		try{
			selectGenresStatement = _mySqlDB.prepareStatement(this.select_genre_sql);
			genresSet = selectGenresStatement.executeQuery();
			while(genresSet.next()){
				Genres aGenre = new Genres(genresSet.getInt(1),genresSet.getString(2));
				genres.add(aGenre);
			}
		}catch(Exception e){
			System.out.println("can not get genre");
			e.printStackTrace();
		}
		return genres;
	}

	/*
	 * This method returns all movie langugages.
	 */
	public List<Languages> getLanguages(){
		List<Languages> languages = new ArrayList<>();
		ResultSet languagesSet = null;
		PreparedStatement selectLanguagesStatement;

		try{
			selectLanguagesStatement = _mySqlDB.prepareStatement(this.select_language_sql);
			languagesSet = selectLanguagesStatement.executeQuery();
			while(languagesSet.next()){
				Languages aLanguages = new Languages(languagesSet.getString(1),languagesSet.getString(2));
				languages.add(aLanguages);
			}
		}catch(Exception e){
			System.out.println("can not get language");
			e.printStackTrace();
		}
		return languages;
	}	

}
