import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

    Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Override
    public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", config.getHost(), config.getPort(), config.getDatabase());
        return DriverManager.getConnection(jdbcUrl, config.getUsername(), config.getPassword());
    }

    @Override
    public List<Movie> queryMovies(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
        logger.info(keywords);

        String movieQuery =
            """
            SELECT t.tconst, t."primaryTitle", t."startYear", t.genres
            FROM tmovies t
            WHERE t."primaryTitle" ILIKE ?
            ORDER BY t."primaryTitle" ASC, t."startYear" ASC
            """;

        List<Movie> movies = new ArrayList<>();

        PreparedStatement ps = connection.prepareStatement(movieQuery);
        ps.setString(1, "%" + keywords + "%"); // Replace ? in movieQuery with '%keyword%'
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            String tconst = rs.getString("tconst");
            String title = rs.getString("primaryTitle");
            int year = rs.getInt("startYear");
            String[] genres = (String[]) rs.getArray("genres").getArray();
            List<String> actorNames = getActorNamesForMovie(connection, tconst);

            Movie movie = new Movie(tconst, title, year, Set.of(genres));
            movie.actorNames.addAll(actorNames);
            movies.add(movie);
        }
        rs.close();
        ps.close();
        return movies;
    }

    public List<String> getActorNamesForMovie(@NotNull Connection connection, @NotNull String tconst) throws SQLException {
        List<String> actorNames = new ArrayList<>();
        String actorNamesQuery =
            """
            SELECT DISTINCT primaryname
            FROM nbasics, tprincipals
            WHERE tprincipals."nconst" = nbasics."nconst" AND
            tprincipals."tconst" = ? AND
            (category = 'actor' OR category = 'actress')
            ORDER BY primaryname ASC;
            """;

        PreparedStatement ps = connection.prepareStatement(actorNamesQuery);
        ps.setString(1, tconst);
        ResultSet rs = ps.executeQuery();
        
        while (rs.next()) {
            actorNames.add(rs.getString("primaryname"));
        }
        rs.close();
        ps.close();
        return actorNames;
    }

    @Override
    public List<Actor> queryActors(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
        logger.info(keywords);

        String actorQuery =
                """
                SELECT b."nconst", b."primaryname", COUNT(DISTINCT p."tconst") as appearances
                FROM nbasics b, tprincipals p
                WHERE b."nconst" = p."nconst" AND 
                b."primaryname" ILIKE ? AND
                (p."category" = 'actor' OR p."category" = 'actress')
                GROUP BY b."nconst", b."primaryname"
                ORDER BY appearances DESC
                LIMIT 5;
                """;

        List<Actor> actors = new ArrayList<>();

        PreparedStatement ps = connection.prepareStatement(actorQuery);
        ps.setString(1, "%" + keywords + "%");
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            Actor actor = new Actor(rs.getString("nconst"), rs.getString("primaryname"));
            actor.playedIn.addAll(getRecentMoviesForActor(connection, actor.nConst));
            actor.costarNameToCount.putAll(getCostarsForActor(connection, actor.nConst));
            actors.add(actor);
        }

        rs.close();
        ps.close();

        return actors;
    }

    public List<String> getRecentMoviesForActor(@NotNull Connection connection, @NotNull String nconst) throws SQLException {

        String movieQuery =
                """
                SELECT DISTINCT m."primaryTitle", m."startYear"
                FROM tprincipals p, tmovies m
                WHERE p."nconst" = ? AND p."tconst" = m."tconst" AND
                (p."category" = 'actor' OR p."category" = 'actress')
                ORDER BY m."startYear" DESC, m."primaryTitle" ASC
                LIMIT 5;
                """;

        List<String> movieNames = new ArrayList<>();

        PreparedStatement ps = connection.prepareStatement(movieQuery);
        ps.setString(1, nconst);
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            movieNames.add(rs.getString("primaryTitle"));
        }

        rs.close();
        ps.close();

        return movieNames;
    }

    public Map<String, Integer> getCostarsForActor(@NotNull Connection connection, @NotNull String nconst) throws SQLException {

        String costarQuery =
                """
                SELECT b."primaryname", COUNT(DISTINCT p1."tconst") AS shared_films
                FROM tprincipals p1, tprincipals p2, nbasics b
                WHERE p1."tconst" = p2."tconst" AND 
                p2."nconst" = b."nconst" AND 
                p1."nconst" = ? AND 
                p2."nconst" != p1."nconst" AND 
                (p1."category" = 'actor' OR p1."category" = 'actress') AND 
                (p2."category" = 'actor' OR p2."category" = 'actress')
                GROUP BY b."primaryname"
                ORDER BY shared_films DESC, b."primaryname" ASC
                LIMIT 5;
                """;

        Map<String, Integer> costars = new LinkedHashMap<>();

        PreparedStatement ps = connection.prepareStatement(costarQuery);
        ps.setString(1, nconst);
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            costars.put(rs.getString("primaryname"), rs.getInt("shared_films"));
        }

        rs.close();
        ps.close();

        return costars;
    }
}
