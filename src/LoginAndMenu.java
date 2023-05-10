import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

public class LoginAndMenu {

	public static void regularMenu() {
		/* prints the choices for commands and parameters */
		System.out.println("\n *** Please enter one of the following commands *** ");
		System.out.println("> search");
		System.out.println("> quit");
	}

	public static void adminMenu() {
		/* prints the choices for commands and parameters */
		System.out.println("\n *** Please enter one of the following commands *** ");
		System.out.println("> search");
		System.out.println("> add");
		System.out.println("> delete");
		System.out.println("> quit");
	}

	public static void searchOptions() {
		/* prints the choices for commands and parameters */
		System.out.println("\n *** Please enter one of the search options *** ");
		System.out.println("> title <movie name>");
		System.out.println("> genre <gennre name>");
	}

	public static void menu(User user, Functions q) {
		/* cid = customer id (obtained from the command line) */

		/* prepare to read the user's command and parameter(s) */
		String response = null;

		while (true) {

			try {

				if(user.getStatus().equals("admin")){
					adminMenu();
				}else{
					regularMenu();
				}	

				BufferedReader r = new BufferedReader(new InputStreamReader(
						System.in));
				/* before prompting the user, tell her/him how many movies he can still rent */
				System.out.print("> ");

				response = r.readLine();

				StringTokenizer st = new StringTokenizer(response);
				String t = st.nextToken();

				if (t.equals("search")) {
					searchOptions();
					System.out.print("> ");
					BufferedReader r2 = new BufferedReader(new InputStreamReader(
						System.in));
					response = r2.readLine();
					st = new StringTokenizer(response);
					String searchOption = st.nextToken();
					if(!searchOption.isEmpty()){
						String keyword = st.nextToken();
						q.transactionSearch(searchOption, keyword);
					}else{
						System.out.println("Error: need to choose a vaid search option");
					}
				} else if (user.getStatus().equals("admin") && t.equals("add")) {
					Movies newMovie = new Movies();
					//movie name is required
					System.out.println("Please Enter Movie Name:");
					BufferedReader r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					String movieTitle = r3.readLine();
					if(movieTitle.isEmpty()){
						System.out.println("Error: need to enter a vaid movie title");
						continue;
					}else{
						newMovie.setTitle(movieTitle);
					}
					
					System.out.println("Budget:");
					r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					double budget = Double.parseDouble(r3.readLine());
					newMovie.setBudget(budget);

					System.out.println("Overview:");
					r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					String overview = r3.readLine();
					newMovie.setOverview(overview);

					System.out.println("Release Date (yyyy-mm-dd) or leave blank:");
					r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					String tmpReleaseDate = r3.readLine();
					if(!tmpReleaseDate.isEmpty()){
						var df = new SimpleDateFormat("yyyy-MM-dd");
						Date releaseDate = df.parse(tmpReleaseDate);
						newMovie.setReleaseDate(releaseDate);
					}

					System.out.println("Revenue:");
					r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					double revenue = Double.parseDouble(r3.readLine());
					newMovie.setRevenue(revenue);

					
					System.out.println("Runtime:");
					r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					int runTime = Integer.parseInt(r3.readLine());
					newMovie.setRunTime(runTime);

					System.out.println("Do you want to add more details? (Y/N):");
					r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					String answer = r3.readLine();

					if(answer.equalsIgnoreCase("y")){
						//list genre or languages with id for selection
						System.out.println("Do you want to add genre? (Y/N):");
						r3 = new BufferedReader(new InputStreamReader(
						System.in));
						answer = r3.readLine();
						if(answer.equalsIgnoreCase("y")){
							List<Genres> genres = q.getGenres();
							if(genres.size() > 0){
								System.out.println("Please select genre id from list of genres:");
								for (Genres aGenres : genres) {
									System.out.println(aGenres.getId() + " " + aGenres.getName());
								}
								r3 = new BufferedReader(new InputStreamReader(
								System.in));
								System.out.print("> ");
								String genreId = r3.readLine();
								newMovie.setGenreId(Integer.parseInt(genreId));
							}
						}

						System.out.println("Do you want to add movie language? (Y/N):");
						r3 = new BufferedReader(new InputStreamReader(
						System.in));
						answer = r3.readLine();
						if(answer.equalsIgnoreCase("y")){
							List<Languages> languages = q.getLanguages();
							if(languages.size() > 0){
								System.out.println("Please select language code from list of genres:");
								for (Languages aLanguages : languages) {
									System.out.println(aLanguages.getLanguageCode() + " " + aLanguages.getName());
								}
								r3 = new BufferedReader(new InputStreamReader(
								System.in));
								System.out.print("> ");
								String languageCode = r3.readLine();
								newMovie.setLanguageCode(languageCode);
							}
						}
					}
					q.addMovie(newMovie);

				}else if (user.getStatus().equals("admin") && t.equals("delete")) {
					System.out.println("Please Enter Full Movie Name:");
					BufferedReader r3 = new BufferedReader(new InputStreamReader(
					System.in));
					System.out.print("> ");
					String movieTitle = r3.readLine();
					if(movieTitle.isEmpty()){
						System.out.println("Error: need to enter a vaid movie title");
						continue;
					}else{
						q.transactionDeleteMovie(movieTitle);
					}
				}else if (t.equals("quit")) {
					System.exit(0);
				}else {
					System.out.println("Error: unrecognized command '" + t
							+ "'");
				}

			} catch (Exception e) {
				//System.out.println("Error: " + e.getMessage());
				 
				q.printTrace(e);
			}
		}
	}

	public static void main(String[] args) {

		if (args.length < 2)
		{
			System.out.println("Usage: java LoginAndMenu USER_EMAIL USER_PASSWORD");
			System.exit(1);
		}
		/* prepare the database connection stuff */
		Functions q = new Functions();
		try {
			q.openConnections();

			/* authenticate the user */
			User user = q.transactionLogin(args[0], args[1]);			
			if (user != null){
				System.out.println("Hi " + user.getName() + " Welcome back to the Movie Base!");
				menu(user, q); /* menu(...) does the real work */
			}else{
				System.out.println("Sorry..."); /* innocent mistake, or malicious attack ? */
			}				
			q.closeConnections();

		} catch (Exception e) {
			q.printTrace(e);
		}

	}

}

