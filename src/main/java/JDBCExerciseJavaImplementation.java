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

            movies.add(new Movie(tconst, title, year, Set.of(genres)));
        }
        rs.close();
        ps.close();
        return movies;
    }

    @Override
    public List<Actor> queryActors(@NotNull Connection connection, @NotNull String keywords) throws SQLException {
        logger.info(keywords);
        List<Actor> actors = new ArrayList<>();

        throw new UnsupportedOperationException("Not yet implemented");
    }
}
