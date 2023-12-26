package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12213012, 12213012, 12213012);
    }
    ////////////////////////////////////////////



//    private final String url = "jdbc:postgresql://localhost:5432/postgres";
//    private final String username = "mker";
//    private final String password = "123456";

    //    private void configureDataSource(String url, String username, String password) {
//        dataSource.
//    }
    private void importDanmuData(List<DanmuRecord> danmuRecords) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "INSERT INTO Danmu_base (BvID, UserID, DisplayTime, Content, PostTime) VALUES (?, ?, ?, ?, ?)"
             )) {
            for (DanmuRecord danmuRecord : danmuRecords) {
                statement.setString(1, danmuRecord.getBv());
                statement.setLong(2, danmuRecord.getMid());
                statement.setFloat(3, danmuRecord.getTime());
                statement.setString(4, danmuRecord.getContent());
                statement.setTimestamp(5, danmuRecord.getPostTime());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void importUserData(List<UserRecord> userRecords) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "INSERT INTO User_base (UserID, Name, Sex, Birthday, Level, Coin, Sign, Identity, Password, QQ, WeChat) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
             )) {
            for (UserRecord userRecord : userRecords) {
                statement.setLong(1, userRecord.getMid());
                statement.setString(2, userRecord.getName());
                statement.setString(3, userRecord.getSex());
                statement.setString(4, userRecord.getBirthday());
                statement.setShort(5, userRecord.getLevel());
                statement.setInt(6, userRecord.getCoin());
                statement.setString(7, userRecord.getSign());
                statement.setString(8, userRecord.getIdentity().name());
                statement.setString(9, userRecord.getPassword());
                statement.setString(10, userRecord.getQq());
                statement.setString(11, userRecord.getWechat());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void importVideoData(List<VideoRecord> videoRecords) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "INSERT INTO Video_base (BvID, Title, OwnerID, OwnerName, CommitTime, ReviewTime, PublicTime, " +
                             "Duration, Description, Reviewer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
             )) {
            for (VideoRecord videoRecord : videoRecords) {
                statement.setString(1, videoRecord.getBv());
                statement.setString(2, videoRecord.getTitle());
                statement.setLong(3, videoRecord.getOwnerMid());
                statement.setString(4, videoRecord.getOwnerName());
                statement.setTimestamp(5, videoRecord.getCommitTime());
                statement.setTimestamp(6, videoRecord.getReviewTime());
                statement.setTimestamp(7, videoRecord.getPublicTime());
                statement.setFloat(8, videoRecord.getDuration());
                statement.setString(9, videoRecord.getDescription());
                statement.setLong(10, videoRecord.getReviewer());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    ////////////////////////////////////////////
    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        // TODO: implement your import logic
//        configureDataSource(url, username, password);
        importUserData(userRecords);
        importVideoData(videoRecords);
        importDanmuData(danmuRecords);
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    //////////////////////////////////////////////////////





}
