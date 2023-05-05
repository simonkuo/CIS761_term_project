package cis761;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
		System.out.println("> delete <Full Movie Name>");
		System.out.println("> quit");
	}

	public static void searchOptions() {
		/* prints the choices for commands and parameters */
		System.out.println("\n *** Please enter one of the search options *** ");
		System.out.println("> name <movie name>");
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
					BufferedReader r2 = new BufferedReader(new InputStreamReader(
						System.in));
					response = r2.readLine();
					st = new StringTokenizer(response);
					String searchOption = st.nextToken();
					if(!searchOption.isEmpty()){
						String keyword = st.nextToken();
						q.transaction_search(searchOption, keyword);
					}else{
						System.out.println("Error: need to choose a vaid search option");
					}
				}

				else if (t.equals("plan")) {
					/* choose a new rental plan, or, if none is given, then list all available plans */
					if (st.hasMoreTokens()) {
						int plan_id = Integer.parseInt(st.nextToken());
						/* need to check that plan_id is a valid plan id in the database, */
						/* if yes, then set the new plan for the current customer */
						/* if not, then list all available plans */
						boolean correct_plan = q.helper_check_plan(plan_id);
						if (correct_plan) {
							System.out.println("Switching to plan " + plan_id);
							q.transaction_choose_plan(cid, plan_id);
						} else {
							System.out.println("Incorrect plan id " + plan_id
									+ "\nAvailable plans are:");
							q.transaction_list_plans();
						}
					} else {
						System.out.println("Available plans:");
						q.transaction_list_plans();
					}
				}

				else if (t.equals("rent")) {
					/* rent the movie with the given movie id */
					String movie_id = st.nextToken("\n").trim();
					System.out.println("Renting the movie id " + movie_id);
					q.transaction_rent(cid, movie_id);
				}

				else if (t.equals("return")) {
					/* return a movie previously rented */
					String movie_id = st.nextToken("\n").trim();
					/* return the movie with mid */
					System.out.println("Returning the movie id " + movie_id);
					q.transaction_return(cid, movie_id);
				}

				else if (t.equals("fastsearch")) {
					/* same as search, only faster */
					if (st.hasMoreTokens()) {
						String movie_title = st.nextToken("\n").trim();
						System.out.println("Fast Searching for the movie '"
								+ movie_title + "'");
						q.transaction_fast_search(cid, movie_title);
					} else {
						System.out
								.println("Error: need to type in movie title");
					}
				}

				else if (t.equals("quit")) {
					System.exit(0);
				}

				else {
					System.out.println("Error: unrecognized command '" + t
							+ "'");
				}

			} catch (Exception e) {
				System.out.println("Error: " + e.getMessage());
			}
		}
	}

	public static void main(String[] args) {

		if (args.length < 2)
		{
			System.out.println("Usage: java SQLassign CUSTOMER_ID CUSTOMER_PASSWORD");
			System.exit(1);
		}
		
		try {

			/* prepare the database connection stuff */
			Functions q = new Functions();
			q.openConnections();
			q.prepareStatements();

			/* authenticate the user */
			User user = q.transaction_login(args[0], args[1]);			
			if (user != null){
				System.out.println("Hi " + user.getName() + " Welcome back to the Movie Base!");
				menu(user, q); /* menu(...) does the real work */
			}else{
				System.out.println("Sorry..."); /* innocent mistake, or malicious attack ? */
			}				
			q.closeConnections();

		} catch (Exception e) {
			System.out.println("Error: " + e.getMessage());
		}

	}

}

