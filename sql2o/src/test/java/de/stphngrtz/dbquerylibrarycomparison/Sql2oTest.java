package de.stphngrtz.dbquerylibrarycomparison;

import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2o;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * http://www.sql2o.org/docs/fetching-data/
 * <p>
 * Disclaimer: Queries might look strange. This is because my goal was to test the functionality, not to write good queries ;)
 */
public class Sql2oTest {

    private static final String db_url = "192.168.99.100";
    private static final String db_port = "5432";
    private static final String db_username = "stephan";
    private static final String db_password = "mysecretpassword";
    private static final String db_database = db_username;

    private Sql2o sql2o;

    @Before
    public void setUp() throws Exception {
        sql2o = new Sql2o("jdbc:postgresql://" + db_url + ":" + db_port + "/" + db_database, db_username, db_password);
    }

    @Test
    public void selectAll() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT *" +
                            "  FROM users")
                            .executeAndFetch(User.class),
                    containsInAnyOrder(
                            new User(1, "Stephan", "stephan.goertz@gmail.com"),
                            new User(2, "Steffi", "steffi05.04@freenet.de"),
                            new User(3, "Willi", "willi@web.de"),
                            new User(4, "Franz", "franz@web.de")
                    )
            );
        }
    }

    @Test
    public void selectAllOrderedBy() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT *" +
                            "  FROM roles r" +
                            " ORDER BY r.name DESC")
                            .executeAndFetch(Role.class),
                    contains(
                            new Role(2, "Developer"),
                            new Role(3, "Designer"),
                            new Role(1, "Admin")
                    )
            );
        }
    }

    @Test
    public void selectWhereEquals() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT *" +
                            "  FROM users u" +
                            " WHERE u.id = :id")
                            .addParameter("id", 1)
                            .executeAndFetch(User.class),
                    contains(
                            new User(1, "Stephan", "stephan.goertz@gmail.com")
                    )
            );
        }
    }

    @Test
    public void selectWhereLike() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT *" +
                            "  FROM users u" +
                            " WHERE u.email LIKE '%@gmail.com'")
                            .executeAndFetch(User.class),
                    contains(
                            new User(1, "Stephan", "stephan.goertz@gmail.com")
                    )
            );

            //c.createQuery("SELECT * from users u where u.email like '%@:xxx'")
            //        .addParameter("xxx", "gmail.com")
            //        .executeAndFetch(User.class)
            //        .forEach(d -> System.out.println(d.id + " " + d.name));
        }
    }

    @Test
    public void selectWithConstant() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u.id, u.name, 'mail@me.de' as email" +
                            "  FROM users u" +
                            " WHERE u.id = :id")
                            .addParameter("id", 1)
                            .executeAndFetch(User.class),
                    contains(
                            new User(1, "Stephan", "mail@me.de")
                    )
            );
        }
    }

    @Test
    public void selectWithSubselectInSelectBlock() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u.name, (SELECT count(*) FROM users_with_roles z WHERE z.user_id = u.id)" +
                            "  FROM users u")
                            .executeAndFetch((ResultSetHandler<String>) rs -> rs.getString(1) + " (" + rs.getString(2) + ")"),
                    containsInAnyOrder(
                            "Stephan (2)",
                            "Steffi (1)",
                            "Willi (0)",
                            "Franz (1)"
                    )
            );
        }
    }

    @Test
    public void selectWithSubselectInWhereBlock() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u.name" +
                            "  FROM users u" +
                            " WHERE u.id IN (SELECT z.user_id FROM users_with_roles z)")
                            .executeAndFetch(String.class),
                    containsInAnyOrder(
                            "Stephan",
                            "Steffi",
                            "Franz"
                    )
            );
        }
    }

    @Test
    public void selectUnionSelect() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u1.id, u1.name" +
                            "  FROM users u1" +
                            " WHERE u1.id = :id1" +
                            " UNION ALL " +
                            "SELECT u2.id, u2.name" +
                            "  FROM users u2" +
                            " WHERE u2.id = :id2")
                            .addParameter("id1", 1)
                            .addParameter("id2", 2)
                            .executeAndFetch(User.class),
                    contains(
                            new User(1, "Stephan", null),
                            new User(2, "Steffi", null)
                    )
            );
        }
    }

    @Test
    public void selectWithImplicitJoin() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u.name, r.name" +
                            "  FROM users u, roles r, users_with_roles z" +
                            " WHERE u.id=z.user_id" +
                            "   AND r.id=z.role_id")
                            .executeAndFetch((ResultSetHandler<String>) rs -> rs.getString(1) + " ist " + rs.getString(2)),
                    containsInAnyOrder(
                            "Stephan ist Admin",
                            "Stephan ist Developer",
                            "Steffi ist Designer",
                            "Franz ist Developer"
                    )
            );
        }
    }

    @Test
    public void selectWithExplicitJoin() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u.name, r.name" +
                            "  FROM users u" +
                            "  JOIN users_with_roles z on z.user_id = u.id" +
                            "  JOIN roles r on r.id = z.role_id")
                            .executeAndFetch((ResultSetHandler<String>) rs -> rs.getString(1) + " ist " + rs.getString(2)),
                    containsInAnyOrder(
                            "Stephan ist Admin",
                            "Stephan ist Developer",
                            "Steffi ist Designer",
                            "Franz ist Developer"
                    )
            );
        }
    }

    @Test
    public void selectWithExplicitLeftOuterJoin() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT u.name, r.name" +
                            "  FROM users u" +
                            "  LEFT OUTER JOIN users_with_roles z on z.user_id = u.id" +
                            "  LEFT OUTER JOIN roles r on r.id = z.role_id")
                            .executeAndFetch((ResultSetHandler<String>) rs -> rs.getString(1) + " ist " + rs.getString(2)),
                    containsInAnyOrder(
                            "Stephan ist Admin",
                            "Stephan ist Developer",
                            "Steffi ist Designer",
                            "Willi ist null",
                            "Franz ist Developer"
                    )
            );
        }
    }

    @Test
    public void selectCountWithGroupBy() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT r.name, count(z.*)" +
                            "  FROM roles r" +
                            "  JOIN users_with_roles z on z.role_id = r.id" +
                            " GROUP BY r.name")
                            .executeAndFetch((ResultSetHandler<String>) rs -> rs.getString(1) + " mit " + rs.getString(2) + " User(n)"),
                    containsInAnyOrder(
                            "Admin mit 1 User(n)",
                            "Developer mit 2 User(n)",
                            "Designer mit 1 User(n)"
                    )
            );
        }
    }

    @Test
    public void selectCountWithGroupByHaving() throws Exception {
        try (Connection c = sql2o.open()) {
            assertThat(c.createQuery("" +
                            "SELECT r.name, count(z.*)" +
                            "  FROM roles r" +
                            "  JOIN users_with_roles z on z.role_id = r.id" +
                            " GROUP BY r.name " +
                            "HAVING count(z.*) > 1")
                            .executeAndFetch((ResultSetHandler<String>) rs -> rs.getString(1) + " mit " + rs.getString(2) + " Usern"),
                    contains(
                            "Developer mit 2 Usern"
                    )
            );
        }
    }
}