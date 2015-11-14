import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
 
import java.io.FileInputStream;
 
/**
 * Runs queries against a back-end database
 */
public class Query {
    private static Properties configProps = new Properties();
 
    private static String imdbUrl;
    private static String customerUrl;
 
    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;
 
    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;
 
    // Canned queries
 
    private String _search_sql = "SELECT * FROM movie WHERE lower(name) like lower(?) ORDER BY id";
    private PreparedStatement _search_statement;
 
    private String _director_mid_sql = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement _director_mid_statement;
   
    private String _actor_mid_sql = "SELECT a.*"
                     + "FROM casts c, actor a "
                     + "WHERE c.mid = ? and c.pid = a.id";
    private PreparedStatement _actor_mid_statement;
   
    private String _rental_mid_sql = "SELECT m.* "
                     + "FROM MovieRentals m "
                     + "WHERE m.mid = ? AND m.status = 'open'";
    private PreparedStatement _rental_mid_statement;
	
	/* fast search queries */
	
	private String _fast_search_sql = "SELECT * FROM movie WHERE lower(name) like lower(?) ORDER BY id";
	private PreparedStatement _fast_search_statement;
	
	private String _fast_director_mid_sql = "SELECT m.id, y.* "
                     + "FROM movie_directors x, directors y, movie m "
                     + "WHERE lower(m.name) LIKE lower(?) AND m.id = x.mid AND x.did = y.id ORDER BY m.id";
    private PreparedStatement _fast_director_mid_statement;
	
	private String _fast_actor_mid_sql = "SELECT m.id, a.* "
					 + "FROM casts c, actor a, movie m "
					 + "WHERE lower(m.name) LIKE lower(?) AND m.id = c.mid AND c.pid = a.id ORDER BY m.id";
	private PreparedStatement _fast_actor_mid_statement;	
 
    /* uncomment, and edit, after your create your own customer database */
   
    private String _customer_login_sql = "SELECT * FROM Customers WHERE login = ? and password = ?";
    private PreparedStatement _customer_login_statement;
 
////////////////
 
    /* SQL to get the customer first and last name */
    private String _customer_data_sql = "SELECT fname, lname FROM Customers WHERE cid = ?";
    private PreparedStatement _customer_data_statement;
 
    /* SQL to get the remaining plan number for a customer */
    private String _customer_plan_number_sql = "SELECT pid FROM Customers WHERE cid = ?";
    private PreparedStatement _customer_plan_number_statement;
 
 
    /*SQL to get the remaining number of user rentals */
    private String _customer_active_movie_rental_count_sql = "SELECT count(mid) FROM MovieRentals WHERE cid = ? AND status LIKE 'open' ";
    private PreparedStatement _customer_active_movie_rental_count;
 
    /* SQL to get the max number of rentals given a plan number */
    private String _plan_maximum_rental_amount_sql = "SELECT max_movies FROM RentalPlans WHERE rid = ?";
    private PreparedStatement _plan_maximum_rental_amount;
 
////////////////
    ///NEW///
    private String _does_movie_exist_sql = "SELECT * FROM Movie WHERE id = ?";
    private PreparedStatement _does_movie_exist;
 
    private String _who_has_this_movie_sql = "SELECT cid FROM MovieRentals WHERE mid = ? AND status LIKE 'open'";
    private PreparedStatement _who_has_this_movie;
 
    private String _return_movie_rental_sql = "UPDATE MovieRentals SET status = 'closed' WHERE mid = ? AND cid = ?";
    private PreparedStatement _return_movie_rental;
 
    ///
    private String _rent_movie_sql = "INSERT INTO MovieRentals values(?, ?, 'open')";
    private PreparedStatement _rent_movie_statement;
 
 
    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;
 
    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;
 
    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
   
 
    public Query() {
    }
 
    /**********************************************************/
    /* Connections to postgres databases */
 
    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
       
       
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");
 
 
        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();
 
        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
 
        _customer_db = DriverManager.getConnection(customerUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
    }
 
    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }
 
    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */
 
    public void prepareStatements() throws Exception {
 
        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
 
        /* uncomment after you create your customers database */
       
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
         
        _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
        _rental_mid_statement = _customer_db.prepareStatement(_rental_mid_sql);
 
		/* Fast Search Statements */
		_fast_search_statement = _imdb.prepareStatement(_fast_search_sql);
		_fast_director_mid_statement = _imdb.prepareStatement(_fast_director_mid_sql);
		_fast_actor_mid_statement = _imdb.prepareStatement(_fast_actor_mid_sql);
 
        /*Helper Method PrepareStatements */
        _customer_data_statement = _customer_db.prepareStatement( _customer_data_sql );
        _customer_plan_number_statement = _customer_db.prepareStatement( _customer_plan_number_sql );
 
        _customer_active_movie_rental_count = _customer_db.prepareStatement( _customer_active_movie_rental_count_sql );
        _plan_maximum_rental_amount = _customer_db.prepareStatement( _plan_maximum_rental_amount_sql );
		
        // NEW
        _return_movie_rental = _customer_db.prepareStatement( _return_movie_rental_sql );
        _does_movie_exist = _imdb.prepareStatement(  _does_movie_exist_sql );
 
        _rent_movie_statement = _customer_db.prepareStatement(_rent_movie_sql);
        _who_has_this_movie = _customer_db.prepareStatement( _who_has_this_movie_sql );
    }
 
 
    /**********************************************************/
    /* suggested helper functions  */
 
    public int helper_compute_remaining_rentals(int cid) throws Exception {
        /* how many movies can she/he still rent ? */
        /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
        int customerRentalPlanMax = -1;
        int customerCurrentRentals = -1;
 
        int customerPlanNumber = helper_compute_customer_plan_number( cid );
        /* Compute the max number of rentals for the given plan */
        _plan_maximum_rental_amount.clearParameters();
        _plan_maximum_rental_amount.setInt( 1, customerPlanNumber );
        ResultSet maxMentalAmount = _plan_maximum_rental_amount.executeQuery();
        if ( maxMentalAmount.next() ){
            customerRentalPlanMax = maxMentalAmount.getInt(1);
        }
 
        _customer_active_movie_rental_count.clearParameters();
        _customer_active_movie_rental_count.setInt( 1, cid );
        ResultSet activeRentalsResults = _customer_active_movie_rental_count.executeQuery();
        if( activeRentalsResults.next() ) {
            customerCurrentRentals = activeRentalsResults.getInt(1);
        }
 
        return (customerRentalPlanMax - customerCurrentRentals);
 
    }
 
    public int helper_compute_customer_plan_number( int cid ) throws Exception {
        int planNumber = -1;
        _customer_plan_number_statement.clearParameters();
        _customer_plan_number_statement.setInt( 1, cid );
 
        ResultSet customer_plan_results = _customer_plan_number_statement.executeQuery();
                  if( customer_plan_results.next() ) {
                    planNumber = customer_plan_results.getInt(1);
                  }
 
        return planNumber;
    }
 
    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        /*Set our parameters in the _customer_data query */
        String data = "";
        _customer_data_statement.clearParameters();
        _customer_data_statement.setInt( 1, cid );
 
        ResultSet customer_data_results = _customer_data_statement.executeQuery();
                 if( customer_data_results.next() ){
                     data = customer_data_results.getString(1) + " " +
                    customer_data_results.getString(2);
                 }
 
        return data;
    }
 
public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        String check_plan = "Select * from RentalPlans where rid="+plan_id;
        PreparedStatement _check_plan = _customer_db.prepareStatement(check_plan);
        return _check_plan.executeQuery().next();
    }
 
    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        _does_movie_exist.clearParameters();
        _does_movie_exist.setInt(1, mid );
        ResultSet movieExists = _does_movie_exist.executeQuery();
 
        if( movieExists.next() ) {
            return true;
        }else{
            return false;
        }
 
    }
 
	public int helper_active_movie_count(int cid) throws Exception{
		String _customer_active_movie_rental_count_sql = "SELECT count(mid) FROM MovieRentals WHERE cid = " + cid + " AND status LIKE 'open' ";
		PreparedStatement _customer_active_movie_rental_count;
		_customer_active_movie_rental_count = _customer_db.prepareStatement(_customer_active_movie_rental_count_sql);
		ResultSet rs = _customer_active_movie_rental_count.executeQuery();
		rs.next();
		return rs.getInt(1);
    }
   
    public int helper_max_movies_allowd_in_plan(int pid) throws Exception{
         String _max_movies_allowed_on_plan_sql = "Select max_movies from RentalPlans where rid = " + pid;
        PreparedStatement _max_movies_allowed = _customer_db.prepareStatement(_max_movies_allowed_on_plan_sql);
		ResultSet rs = _max_movies_allowed.executeQuery();
		rs.next();
        return rs.getInt(1);
    }
 
    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        _who_has_this_movie.clearParameters();
        _who_has_this_movie.setInt( 1, mid );
        ResultSet movieOwnerResults = _who_has_this_movie.executeQuery();
 
        if( movieOwnerResults.next() ) {
            return movieOwnerResults.getInt(1);
        }else{
            return -1;
        }
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
        return(cid);
       
        //return (55);
    }
 
    public void transaction_personal_data(int cid) throws Exception {
        /* println the customer's personal data: name, and plan number */
 
        /*Get the customer name */
        String customerFullName = helper_compute_customer_name( cid );
        int planNumber = helper_compute_customer_plan_number( cid );
        int remainingCustomerRentals = helper_compute_remaining_rentals( cid );
 
        System.out.println(customerFullName + " - Plan #: " + planNumber + " -  Remaining Rentals: " + helper_compute_remaining_rentals( cid ) );
       
    }
 
 
    /**********************************************************/
    /* main functions in this project: */
 
    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */
 
        /* set the first (and single) '?' parameter */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');
 
        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();
            /* now you need to retrieve the actors, in the same manner */
            _actor_mid_statement.clearParameters();
            _actor_mid_statement.setInt(1, mid);
            ResultSet actor_set = _actor_mid_statement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\tActor: " + actor_set.getString(2)
                        + " " + actor_set.getString(3));
            }
            actor_set.close();
            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
            _rental_mid_statement.clearParameters();
            _rental_mid_statement.setInt(1, mid);
            ResultSet rental_set = _rental_mid_statement.executeQuery();
            if(rental_set.next()) {
                do {
					String status;
					if(rental_set.getString(3).equals("open")) status = "Not Available";
					else status = "Available";						
					System.out.println("\t\tRental Status: " + status);                   
					if(helper_who_has_this_movie(rental_set.getInt(1)) == cid){
						System.out.println("\t\tYou've already rented this movie out");
                    }
                } while (rental_set.next());
            }
            else{
                System.out.println("\t\tRental Status: Available");
            }
            rental_set.close();
        }
        System.out.println();
    }
 
public void transaction_choose_plan(int cid, int pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
        /* remember to enforce consistency ! */
        int curMov = helper_active_movie_count(cid);
        if(helper_check_plan(pid)){
            int numMaxPlans = helper_max_movies_allowd_in_plan(pid);
        String _choose_plan = "UPDATE Customers set pid =" + pid
        + " Where cid=" + cid;
        PreparedStatement _choose_plan_statement = _customer_db.prepareStatement(_choose_plan);
       
        //if customers plan max is less then
       
        int  a = helper_compute_remaining_rentals(cid);
       
            if(curMov > numMaxPlans){
                System.out.println("You have more movies rented then movies allowed by the plan");
            }
            else{
				_choose_plan_statement.executeUpdate();
				System.out.println("Switched plans successfully");
            }
        }
        else{
            System.out.println("Wrong plan id, please pick a valid plan id");
        }
       
       
    }
 
    public void transaction_list_plans() throws Exception {
        /* println all available plans: SELECT * FROM plan */
       
        String _list_plans = "Select Distinct name,max_movies,price from RentalPlans";
        PreparedStatement _prep_list_plan= _customer_db.prepareStatement(_list_plans);  
        ResultSet list_plans = _prep_list_plan.executeQuery();
       
        while(list_plans.next()){
            System.out.println("Name: "+list_plans.getString(1)+" Max movies: "+list_plans.getInt(2)+" Price: "+list_plans.getDouble(3));
        }
   
    }
   
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
		String _list_movie_rentals = "SELECT * FROM MovieRentals WHERE cid = " + cid + " AND status = 'open'";
        PreparedStatement _prep_movie_rentals_list = _customer_db.prepareStatement(_list_movie_rentals);  
        ResultSet list_rentals = _prep_movie_rentals_list.executeQuery();
		
		while(list_rentals.next()){
			_does_movie_exist.clearParameters();
			_does_movie_exist.setInt(1, list_rentals.getInt(1));
			ResultSet movieExists = _does_movie_exist.executeQuery();
			while(movieExists.next()){
				 System.out.println("\t\t " + movieExists.getInt(1) + " | " + movieExists.getString(2));
			}
			movieExists.close();
        }
		list_rentals.close();
    }
 
	public void transaction_rent(int cid, int mid) throws Exception {
        /* rend the movie mid to the customer cid */
        /* remember to enforce consistency ! */
        _begin_transaction_read_write_statement.executeUpdate();
        _rent_movie_statement.clearParameters();
        _rent_movie_statement.setInt(1, mid);
        _rent_movie_statement.setInt(2, cid);
        if(helper_check_movie(mid)){
            if(helper_who_has_this_movie(mid) == -1){
				if(helper_compute_remaining_rentals(cid) > 0){
					_rent_movie_statement.execute();
					_commit_transaction_statement.executeUpdate();
					System.out.println("Movie successfully rented");
				}
				else{
					System.out.println("You cannot rent more movies with your current plan");
				}
            }
            else{
                System.out.println("Movie is already rented out");
                  _rollback_transaction_statement.executeUpdate();
            }
        }
        else{
            System.out.println("Invalid movie ID");
             _rollback_transaction_statement.executeUpdate();
        }
    }
//  private String _return_movie_rental_sql = "UPDATE MovieRentals SET status = 'closed' WHERE mid = ? AND cid = ?"
    public void transaction_return(int cid, int mid) throws Exception {
        /* return the movie mid by the customer cid */
 
          _begin_transaction_read_write_statement.executeUpdate();
 
          /* Check that you are returning a valid movie */
          if( !helper_check_movie( mid ) ){
            System.out.println("This movie does not exist.");
            /* Rollback */
            _rollback_transaction_statement.executeUpdate();
            return;
          }
 
          /* Checking for the owner of the movie */
          if( helper_who_has_this_movie( mid ) != cid ) {
            System.out.println("This movie is not in your possession");
            _rollback_transaction_statement.executeUpdate();
            return;
          }
 
          _return_movie_rental.clearParameters();
          _return_movie_rental.setInt(1, mid);
          _return_movie_rental.setInt(2, cid);
          _return_movie_rental.executeUpdate();
 
          // commit the changes
           _commit_transaction_statement.executeUpdate();
 
           System.out.println("Movie id " + mid + " returned.");
 
 
    }
 
    public void transaction_fast_search(int cid, String movie_title)
            throws Exception {
        /* like transaction_search, but uses joins instead of independent joins
           Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
           Answers are sorted by mid.
           Then merge-joins the three answer sets */
		
		int mid;
		
		_fast_search_statement.clearParameters();
        _fast_search_statement.setString(1, '%' + movie_title + '%');
        ResultSet movie_set = _fast_search_statement.executeQuery();
		
		_fast_director_mid_statement.clearParameters();
		_fast_director_mid_statement.setString(1, '%' + movie_title + '%');
        ResultSet director_set = _fast_director_mid_statement.executeQuery();

		_fast_actor_mid_statement.clearParameters();
        _fast_actor_mid_statement.setString(1, '%' + movie_title + '%');
        ResultSet actor_set = _fast_actor_mid_statement.executeQuery();
		
		while(movie_set.next()){
			mid = movie_set.getInt(1);
			System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
			while(director_set.next() && director_set.getInt(1) == mid){
				System.out.println("\t\tDirector: " + director_set.getString(4)
                        + " " + director_set.getString(3));
			}
			while(actor_set.next() && actor_set.getInt(1) == mid){
				System.out.println("\t\tActor: " + actor_set.getString(3)
                        + " " + actor_set.getString(4));
			}			
		}
		
		movie_set.close();
		director_set.close();
		actor_set.close();
    }
 
}