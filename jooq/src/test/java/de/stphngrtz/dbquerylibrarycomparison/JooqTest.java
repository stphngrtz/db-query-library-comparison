package de.stphngrtz.dbquerylibrarycomparison;

import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListener;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static de.stphngrtz.dbquerylibrarycomparison.tables.Roles.ROLES;
import static de.stphngrtz.dbquerylibrarycomparison.tables.Users.USERS;
import static de.stphngrtz.dbquerylibrarycomparison.tables.UsersWithRoles.USERS_WITH_ROLES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * http://www.jooq.org/doc/3.7/manual-single-page/
 * <p>
 * Disclaimer: Queries might look strange. This is because my goal was to test the functionality, not to write good queries ;)
 */
public class JooqTest {

    private static final String db_url = "192.168.99.100";
    private static final String db_port = "5432";
    private static final String db_username = "stephan";
    private static final String db_password = "mysecretpassword";
    private static final String db_database = db_username;

    private Connection connection;
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:postgresql://" + db_url + ":" + db_port + "/" + db_database, db_username, db_password);
        configuration = new DefaultConfiguration()
                .set(connection)
                .set(SQLDialect.POSTGRES)
                .set(new DefaultExecuteListenerProvider(new DefaultExecuteListener() {
                    @Override
                    public void executeStart(ExecuteContext ctx) {
                        System.out.println(ctx.sql());
                    }
                }));
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void selectAll() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select()
                        .from(USERS)
                        .fetch()
                        .map(r -> new User(r.getValue(USERS.ID), r.getValue(USERS.NAME), r.getValue(USERS.EMAIL))),
                containsInAnyOrder(
                        new User(1, "Stephan", "stephan.goertz@gmail.com"),
                        new User(2, "Steffi", "steffi05.04@freenet.de"),
                        new User(3, "Willi", "willi@web.de"),
                        new User(4, "Franz", "franz@web.de")
                )
        );
    }

    @Test
    public void selectAllOrderedBy() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select()
                        .from(ROLES)
                        .orderBy(ROLES.NAME.desc())
                        .fetch()
                        .map(r -> new Role(r.getValue(ROLES.ID), r.getValue(ROLES.NAME))),
                contains(
                        new Role(2, "Developer"),
                        new Role(3, "Designer"),
                        new Role(1, "Admin")
                )
        );
    }

    @Test
    public void selectWhereEquals() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select()
                        .from(USERS)
                        .where(USERS.ID.eq(1))
                        .fetch()
                        .map(t -> new User(t.getValue(USERS.ID), t.getValue(USERS.NAME), t.getValue(USERS.EMAIL))),
                contains(
                        new User(1, "Stephan", "stephan.goertz@gmail.com")
                )
        );
    }

    @Test
    public void selectWhereLike() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select()
                        .from(USERS)
                        .where(USERS.EMAIL.like("%@gmail.com"))
                        .fetch()
                        .map(t -> new User(t.getValue(USERS.ID), t.getValue(USERS.NAME), t.getValue(USERS.EMAIL))),
                contains(
                        new User(1, "Stephan", "stephan.goertz@gmail.com")
                )
        );
    }

    @Test
    public void selectWithConstant() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(USERS.ID, USERS.NAME, DSL.val("mail@me.de"))
                        .from(USERS)
                        .where(USERS.ID.eq(1))
                        .fetch()
                        .map(t -> new User(t.getValue(USERS.ID), t.getValue(USERS.NAME), t.getValue(2, String.class))),
                contains(
                        new User(1, "Stephan", "mail@me.de")
                )
        );
    }

    @Test
    public void selectWithSubselectInSelectBlock() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(USERS.NAME, DSL.using(configuration).select(DSL.count()).from(USERS_WITH_ROLES).where(USERS_WITH_ROLES.USER_ID.eq(USERS.ID)).asField())
                        .from(USERS)
                        .fetch()
                        .map(t -> t.getValue(USERS.NAME) + " (" + t.getValue(1, Integer.class) + ")"),
                containsInAnyOrder(
                        "Stephan (2)",
                        "Steffi (1)",
                        "Willi (0)",
                        "Franz (1)"
                )
        );
    }

    @Test
    public void selectWithSubselectInWhereBlock() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(USERS.NAME)
                        .from(USERS)
                        .where(USERS.ID.in(DSL.using(configuration).select(USERS_WITH_ROLES.USER_ID).from(USERS_WITH_ROLES)))
                        .fetch()
                        .map(r -> r.getValue(USERS.NAME)),
                containsInAnyOrder(
                        "Stephan",
                        "Steffi",
                        "Franz"
                )
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectUnionSelect() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL.query()
                        .select(USERS.ID, USERS.NAME)
                        .from(USERS)
                        .where(USERS.ID.eq(1))
                        .union(
                                DSL.using(configuration)
                                        .select(USERS.ID, USERS.NAME)
                                        .from(USERS)
                                        .where(USERS.ID.eq(2))
                        )
                        .fetch()
                        .map(r -> new User(r.getValue(USERS.ID), r.getValue(USERS.NAME), null)),
                contains(
                        new User(1, "Stephan", null),
                        new User(2, "Steffi", null)
                )
        );
    }

    @Test
    public void selectWithImplicitJoin() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(USERS.NAME, ROLES.NAME)
                        .from(USERS, ROLES, USERS_WITH_ROLES)
                        .where(
                                USERS.ID.eq(USERS_WITH_ROLES.USER_ID),
                                ROLES.ID.eq(USERS_WITH_ROLES.ROLE_ID)
                        )
                        .fetch()
                        .map(t -> t.getValue(USERS.NAME) + " ist " + t.getValue(ROLES.NAME)),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer",
                        "Franz ist Developer"
                )
        );
    }

    @Test
    public void selectWithExplicitJoin() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(USERS.NAME, ROLES.NAME)
                        .from(USERS)
                        .join(USERS_WITH_ROLES).on(USERS_WITH_ROLES.USER_ID.eq(USERS.ID))
                        .join(ROLES).on(ROLES.ID.eq(USERS_WITH_ROLES.ROLE_ID))
                        .fetch()
                        .map(t -> t.getValue(USERS.NAME) + " ist " + t.getValue(ROLES.NAME)),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer",
                        "Franz ist Developer"
                )
        );
    }

    @Test
    public void selectWithExplicitLeftOuterJoin() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(USERS.NAME, ROLES.NAME)
                        .from(USERS)
                        .leftJoin(USERS_WITH_ROLES).on(USERS_WITH_ROLES.USER_ID.eq(USERS.ID))
                        .leftJoin(ROLES).on(ROLES.ID.eq(USERS_WITH_ROLES.ROLE_ID))
                        .fetch()
                        .map(t -> t.getValue(USERS.NAME) + " ist " + t.getValue(ROLES.NAME)),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer",
                        "Willi ist null",
                        "Franz ist Developer"
                )
        );
    }

    @Test
    public void selectCountWithGroupBy() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(ROLES.NAME, DSL.count(USERS_WITH_ROLES))
                        .from(ROLES)
                        .join(USERS_WITH_ROLES).on(USERS_WITH_ROLES.ROLE_ID.eq(ROLES.ID))
                        .groupBy(ROLES.NAME)
                        .fetch()
                        .map(t -> t.getValue(ROLES.NAME) + " mit " + t.getValue(1, Integer.class) + " User(n)"),
                containsInAnyOrder(
                        "Admin mit 1 User(n)",
                        "Developer mit 2 User(n)",
                        "Designer mit 1 User(n)"
                )
        );
    }

    @Test
    public void selectCountWithGroupByHaving() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(ROLES.NAME, DSL.count(USERS_WITH_ROLES))
                        .from(ROLES)
                        .join(USERS_WITH_ROLES).on(USERS_WITH_ROLES.ROLE_ID.eq(ROLES.ID))
                        .groupBy(ROLES.NAME)
                        .having(DSL.count(USERS_WITH_ROLES).gt(1))
                        .fetch()
                        .map(t -> t.getValue(ROLES.NAME) + " mit " + t.getValue(1, Integer.class) + " User(n)"),
                contains(
                        "Developer mit 2 User(n)"
                )
        );
    }

    @Test
    public void window() throws Exception {
        // siehe http://blog.jooq.org/2013/11/03/probably-the-coolest-sql-feature-window-functions/
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(
                                USERS.NAME,
                                DSL.left(USERS.NAME, 1),
                                DSL.count().over().partitionBy(DSL.left(USERS.NAME, 1))
                        )
                        .from(USERS)
                        .orderBy(USERS.NAME.asc())
                        .fetch()
                        .map(t -> t.getValue(0, String.class) + " " + t.getValue(1, String.class) + " " + t.getValue(2, Integer.class)),
                contains(
                        "Franz F 1",
                        "Steffi S 2",
                        "Stephan S 2",
                        "Willi W 1"
                )
        );
    }

    @Test
    public void multipleWindows() throws Exception {
        // siehe http://blog.jooq.org/2013/11/03/probably-the-coolest-sql-feature-window-functions/
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .select(
                                DSL.lag(USERS.NAME).over().orderBy(USERS.NAME.asc()),
                                USERS.NAME,
                                DSL.lead(USERS.NAME).over().orderBy(USERS.NAME.asc())
                        )
                        .from(USERS)
                        .orderBy(USERS.NAME.asc())
                        .fetch()
                        .map(t -> t.getValue(0, String.class) + " <- " + t.getValue(1, String.class) + " -> " + t.getValue(2, String.class)),
                contains(
                        "null <- Franz -> Steffi",
                        "Franz <- Steffi -> Stephan",
                        "Steffi <- Stephan -> Willi",
                        "Stephan <- Willi -> null"
                )
        );
    }

    @Test
    public void selectFromSimpleCommonTable() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL.query()
                        .with("common").as(DSL.using(configuration)
                                .select(USERS.NAME, USERS.EMAIL)
                                .from(USERS)
                                .where(USERS.NAME.startsWith("S"))
                        )
                        .select()
                        .from(DSL.table(DSL.name("common")))
                        .fetch()
                        .map(t -> t.getValue(0, String.class) + " (" + t.getValue(1, String.class) + ")"),
                containsInAnyOrder(
                        "Stephan (stephan.goertz@gmail.com)",
                        "Steffi (steffi05.04@freenet.de)"
                )
        );
    }

    @Test
    public void selectFromMoreComplexCommonTable() throws Exception {
        CommonTableExpression<Record2<String, String>> common = DSL
                .name("common")
                .fields("userName", "roleName")
                .as(DSL.using(configuration)
                        .select(USERS.NAME.as("userName"), ROLES.NAME.as("roleName"))
                        .from(USERS)
                        .leftJoin(USERS_WITH_ROLES).on(USERS_WITH_ROLES.USER_ID.eq(USERS.ID))
                        .leftJoin(ROLES).on(ROLES.ID.eq(USERS_WITH_ROLES.ROLE_ID))
                );
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL.query()
                        .with(common)
                        .select()
                        .from(common)
                        .where(common.field("userName", String.class).startsWith("S"))
                        .fetch()
                        .map(t -> t.getValue(common.field("userName", String.class)) + " ist " + t.getValue(common.field("roleName", String.class))),
                containsInAnyOrder(
                        "Stephan ist Admin",
                        "Stephan ist Developer",
                        "Steffi ist Designer"
                )
        );
    }

    @Test
    public void dmlStatements() throws Exception {
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .insertInto(USERS)
                        .columns(USERS.ID, USERS.NAME, USERS.EMAIL)
                        .values(5, "Test", "test@mail.de")
                        .execute(),
                equalTo(1)
        );
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .update(USERS)
                        .set(USERS.EMAIL, "test@web.de")
                        .where(USERS.ID.eq(5))
                        .execute(),
                equalTo(1)
        );
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .update(USERS)
                        .set(USERS.NAME, "Testuser")
                        .where(USERS.EMAIL.eq("test@mail.de"))
                        .execute(),
                equalTo(0)
        );
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        .delete(USERS)
                        .where(USERS.ID.eq(5))
                        .execute(),
                equalTo(1)
        );
    }

    @Test
    public void dmlBatchStatements() throws Exception {
        assertThat(DSL.using(configuration)
                        // batch with single query
                        .batch(DSL.using(configuration).insertInto(USERS).columns(USERS.ID, USERS.NAME, USERS.EMAIL).values((Integer) null, null, null))
                        .bind(5, "Test 1", "test1@mail.de")
                        .bind(6, "Test 2", "test2@mail.de")
                        .execute(),
                equalTo(new int[]{1, 1})
        );
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        // batch with several queries
                        .batch(
                                DSL.using(configuration).update(USERS).set(USERS.NAME, "Testuser 1").where(USERS.ID.eq(5)),
                                DSL.using(configuration).update(USERS).set(USERS.NAME, "Testuser 2").where(USERS.ID.eq(6))
                        )
                        .execute(),
                equalTo(new int[]{1, 1})
        );
        assertThat(DSL.using(configuration) // or PostgresDSLto use dialect-specific DSL
                        // batch with several queries
                        .batch(
                                DSL.using(configuration).delete(USERS).where(USERS.ID.eq(5)),
                                DSL.using(configuration).delete(USERS).where(USERS.ID.eq(6))
                        )
                        .execute(),
                equalTo(new int[]{1, 1})
        );
    }
}