package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;


    public boolean Authisvalid(AuthInfo auth)
    {
        if(auth == null || auth.getMid() < 0 || auth.getPassword() == null || auth.getPassword().isEmpty())
        {
            return false;
        }

        String sql = "SELECT * FROM user_base WHERE UserID = ? AND Password = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, auth.getPassword());
            ResultSet rs = stmt.executeQuery();
            //判断qq和wechat是否对应同一个用户
            if(rs.next())
            {
                if(auth.getQq() != null && !auth.getQq().isEmpty())
                {
                    String sql1 = "SELECT * FROM User_base WHERE UserID = ? AND QQ = ?";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                        stmt1.setLong(1, auth.getMid());
                        stmt1.setString(2, auth.getQq());
                        ResultSet rs1 = stmt1.executeQuery();
                        if(!rs1.next())
                        {
                            return false;
                        }
                    } catch (SQLException e) {
                        return false;
                    }
                }
                if(auth.getWechat() != null && !auth.getWechat().isEmpty())
                {
                    String sql2 = "SELECT * FROM User_base WHERE UserID = ? AND WeChat = ?";
                    try (Connection conn2 = dataSource.getConnection();
                         PreparedStatement stmt2 = conn2.prepareStatement(sql2)) {
                        stmt2.setLong(1, auth.getMid());
                        stmt2.setString(2, auth.getWechat());
                        ResultSet rs2 = stmt2.executeQuery();
                        if(!rs2.next())
                        {
                            return false;
                        }
                    } catch (SQLException e) {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Recommends a list of top 5 similar videos for a video.
     * The similarity is defined as the number of users (in the database) who have watched both videos.
     *
     * @param bv the current video
     * @return a list of video {@code bv}s
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<String> recommendNextVideo(String bv)
    {
        if (bv == null || bv.isEmpty())
        {
            return null;
        }
        //能不能找到bv对应的视频
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT BvID FROM Video_base WHERE BvID = ?"

             )) {
            statement.setString(1, bv);
            ResultSet resultSet = statement.executeQuery();
            if (!resultSet.next()) {
                return null;
            }
        } catch (SQLException e) {
            log.error("Failed to recommend next video for {}", bv, e);
            return null;
        }
        //找到了bv对应的视频,排序如果相似度一样，那么按照bv的顺序排序
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT BvID FROM Video_base WHERE BvID IN (SELECT BvID FROM video_viewer " +
                             "WHERE UserID IN (SELECT UserID FROM video_viewer WHERE BvID = ?)) AND BvID != ? " +
                                "ORDER BY (SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID AND UserID IN (SELECT UserID FROM video_viewer WHERE BvID = ?)) DESC, BvID LIMIT 5"
             )) {
            statement.setString(1, bv);
            statement.setString(2, bv);
            statement.setString(3, bv);
            ResultSet resultSet = statement.executeQuery();
            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;
        } catch (SQLException e) {
            log.error("Failed to recommend next video for {}", bv, e);
            return null;
        }

    }

    /**
     * Recommends videos for anonymous users, based on the popularity.
     * Evaluate the video's popularity from the following aspects:
     * <ol>
     *   <li>"like": the rate of watched users who also liked this video</li>
     *   <li>"coin": the rate of watched users who also donated coin to this video</li>
     *   <li>"fav": the rate of watched users who also collected this video</li>
     *   <li>"danmu": the average number of danmus sent by one watched user</li>
     *   <li>"finish": the average video watched percentage of one watched user</li>
     * </ol>
     * The recommendation score can be calculated as:
     * <pre>
     *   score = like + coin + fav + danmu + finish
     * </pre>
     *
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s, sorted by the recommendation score
     * @implNote
     * Though users can like/coin/favorite a video without watching it, the rates of these values should be clamped to 1.
     * If no one has watched this video, all the five scores shall be 0.
     * If the requested page is empty, return an empty list.
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li> //done
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum)
    {
        if (pageSize <= 0 || pageNum <= 0)
        {
            return null;
        }
        //WHERE ((SELECT COUNT(*) FROM video_viewer WHERE bvid = video_base.bvid)!=0)
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT BvID FROM Video_base  " +
                             "ORDER BY (" +
                             "(SELECT CAST(COUNT(*) AS DOUBLE PRECISION) AS result FROM video_viewer WHERE BvID = Video_base.BvID AND userid IN (SELECT userid FROM video_like WHERE video_base.bvid = video_like.bvid) / (SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID ))" //like
                             + "+ (SELECT CAST(COUNT(*) AS DOUBLE PRECISION) AS result FROM video_viewer WHERE BvID = Video_base.BvID AND userid IN (SELECT userid FROM video_coin WHERE video_base.bvid = video_coin.bvid) / (SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID ) )" //coin
                                + "+ (SELECT CAST(COUNT(*) AS DOUBLE PRECISION) AS result FROM video_viewer WHERE BvID = Video_base.BvID AND userid IN (SELECT userid FROM video_favorite WHERE video_base.bvid = video_favorite.bvid) / (SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID ))" //favortie
                                        + "+ ((SELECT CAST(COUNT(*) AS DOUBLE PRECISION) AS result FROM danmu_base WHERE BvID = Video_base.BvID) / (SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID ))" //danmu
                                                + "+ ((SELECT CAST(SUM(time) AS DOUBLE PRECISION) AS result FROM video_viewer WHERE BvID = Video_base.BvID) / ((SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID ) * video_base.duration))" //finish
                                                        + "* CASE WHEN (SELECT COUNT(*) FROM video_viewer WHERE BvID = Video_base.BvID) = 0 THEN 0 ELSE 1 END )" +
                             "DESC LIMIT ? OFFSET ?"
                     )) {
            statement.setInt(1, pageSize);
            statement.setInt(2, (pageNum - 1) * pageSize);
            ResultSet resultSet = statement.executeQuery();
            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;
        } catch (SQLException e) {
            log.error("Failed to recommend general videos", e);
            return null;
        }
    }

    /**
     * Recommends videos for a user, restricted on their interests.
     * The user's interests are defined as the videos that the user's friend(s) have watched,
     * filter out the videos that the user has already watched.
     * Friend(s) of current user is/are the one(s) who is/are both the current user' follower and followee at the same time.
     * Sort the videos by:
     * <ol>
     *   <li>The number of friends who have watched the video</li>
     *   <li>The video owner's level</li>
     *   <li>The video's public time (newer videos are preferred)</li>
     * </ol>
     *
     * @param auth     the current user's authentication information to be recommended
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s
     * @implNote
     * If the current user's interest is empty, return {@link io.sustc.service.RecommenderService#generalRecommendations(int, int)}. //done
     * If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li> //done
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum)
    {
        if(auth == null || auth.getMid() < 0 || auth.getPassword() == null || auth.getPassword().isEmpty() || pageSize <= 0 || pageNum <= 0 || !Authisvalid(auth))
        {
            return null;
        }
        //找出该用户朋友已经看过的视频
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT BvID FROM video_viewer WHERE userid IN (SELECT followerid FROM user_follow WHERE upid = ? AND followerid IN (SELECT upid FROM user_follow WHERE followerid = ?))"
                             )) {
            statement.setLong(1, auth.getMid());
            statement.setLong(2, auth.getMid());
            ResultSet resultSet = statement.executeQuery();
            if(!resultSet.next())
            {
                return generalRecommendations(pageSize, pageNum);
            }
            else
            {
                try (Connection conn1 = dataSource.getConnection();
                     PreparedStatement statement1 = conn1.prepareStatement(
                             "SELECT BvID FROM video_base WHERE BvID IN " +
                                     "(SELECT BvID FROM video_viewer WHERE userid IN (SELECT followerid FROM user_follow WHERE upid = ? AND followerid IN (SELECT upid FROM user_follow WHERE followerid = ?))) " +
                                     "AND BvID NOT IN (SELECT BvID FROM video_viewer WHERE userid = ?) " +
                                     "ORDER BY (SELECT COUNT(*) FROM video_viewer WHERE BvID = video_base.BvID AND userid IN (SELECT followerid FROM user_follow WHERE upid = ? AND followerid IN (SELECT upid FROM user_follow WHERE followerid = ?))) DESC, " +
                                     "(SELECT Level FROM user_base WHERE UserID = video_base.ownerid) DESC, " +
                                     "publictime DESC LIMIT ? OFFSET ?"
                     )) {
                    statement1.setLong(1, auth.getMid());
                    statement1.setLong(2, auth.getMid());
                    statement1.setLong(3, auth.getMid());
                    statement1.setLong(4, auth.getMid());
                    statement1.setLong(5, auth.getMid());
                    statement1.setInt(6, pageSize);
                    statement1.setInt(7, (pageNum - 1) * pageSize);
                    ResultSet resultSet1 = statement1.executeQuery();
                    List<String> result = new ArrayList<>();
                    while (resultSet1.next()) {
                        result.add(resultSet1.getString(1));
                    }
                    return result;
                } catch (SQLException e) {
                    log.error("Failed to recommend videos for {}", auth.getMid(), e);
                    return null;
                }
            }
        } catch (SQLException e) {
            log.error("Failed to recommend videos for {}", auth.getMid(), e);
            return null;
        }

    }

    /**
     * Recommends friends for a user, based on their common followings.
     * Find all users that are not currently followed by the user, and have at least one common following with the user.
     * Sort the users by the number of common followings, if two users have the same number of common followings,
     * sort them by their {@code level}.
     *
     * @param auth     the current user's authentication information to be recommended
     * @param pageSize the page size, if there are less than {@code pageSize} users, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of {@code mid}s of the recommended users
     * @implNote If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum)
    {
        if(auth == null || auth.getMid() < 0 || auth.getPassword() == null || auth.getPassword().isEmpty() || pageSize <= 0 || pageNum <= 0 || !Authisvalid(auth))
        {
            return null;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT UserID FROM User_base WHERE UserID IN (SELECT UserID FROM user_follow " +
                             "WHERE FollowerID IN (SELECT FollowerID FROM user_follow WHERE UserID = ?)) AND UserID NOT IN (SELECT FollowerID FROM user_follow WHERE UserID = ?) " +
                             "ORDER BY (SELECT COUNT(*) FROM user_follow WHERE UserID = User_base.UserID AND FollowerID IN (SELECT FollowerID FROM user_follow WHERE UserID = ?)) DESC, Level DESC LIMIT ? OFFSET ?"
             )) {
            statement.setLong(1, auth.getMid());
            statement.setLong(2, auth.getMid());
            statement.setLong(3, auth.getMid());
            statement.setInt(4, pageSize);
            statement.setInt(5, (pageNum - 1) * pageSize);
            ResultSet resultSet = statement.executeQuery();
            List<Long> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getLong(1));
            }
            return result;
        } catch (SQLException e) {
            log.error("Failed to recommend friends for {}", auth.getMid(), e);
            return null;
        }
    }

}
