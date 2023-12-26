package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {

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
        String sql = "SELECT * FROM User_base WHERE UserID = ? AND Password = ?";
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
     * Sends a danmu to a video.
     * It is mandatory that the user shall watch the video first before he/she can send danmu to it.
     *
     * @param auth    the current user's authentication information
     * @param bv      the video's bv
     * @param content the content of danmu
     * @param time    seconds since the video starts
     * @return the generated danmu id
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>{@code content} is invalid (null or empty)</li> //done
     *   <li>the video is not published or the user has not watched this video</li> //done
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time)
    {
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty() || content == null || content.isEmpty() || time < 0)
        {
            return -1;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT * FROM Video_base WHERE BvID = ?"
             )) {
            statement.setString(1, bv);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next())
            {
                //比较publictime和当前时间
                Timestamp publictime = resultSet.getTimestamp("PublicTime");
                if(publictime == null)
                {
                    return -1;
                }
                if(publictime.after(Timestamp.valueOf(LocalDateTime.now())))
                {
                    return -1;
                }
                //是否观看
                String sql1 = "SELECT * FROM video_viewer WHERE UserID = ? AND BvID = ?";
                try (Connection conn1 = dataSource.getConnection();
                     PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                    stmt1.setLong(1, auth.getMid());
                    stmt1.setString(2, bv);
                    ResultSet rs1 = stmt1.executeQuery();
                    if(!rs1.next())
                    {
                        return -1;
                    }
                } catch (SQLException e) {
                    return -1;
                }
                //插入弹幕
                String sql2 = "INSERT INTO Danmu_base (BvID, UserID, displaytime, Content, PostTime) VALUES (?, ?, ?, ?, ?)"+
                        "RETURNING DanmuID";
                try (Connection conn2 = dataSource.getConnection();
                     PreparedStatement stmt2 = conn2.prepareStatement(sql2)) {
                    stmt2.setString(1, bv);
                    stmt2.setLong(2, auth.getMid());
                    stmt2.setFloat(3, time);
                    stmt2.setString(4, content);
                    stmt2.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
                    stmt2.executeUpdate();
                    ResultSet rs2 = stmt2.executeQuery();
                    return rs2.getLong(1);
                } catch (SQLException e) {
                    return -1;
                }
            }
            else
            {
                return -1;
            }
        } catch (SQLException e) {
            return -1;
        }
    }

    /**
     * Display the danmus in a time range.
     * Similar to bilibili's mechanism, user can choose to only display part of the danmus to have a better watching
     * experience.
     *
     * @param bv        the video's bv
     * @param timeStart the start time of the range
     * @param timeEnd   the end time of the range
     * @param filter    whether to remove the duplicated content,
     *                  if {@code true}, only the earliest posted danmu with the same content shall be returned
     * @return a list of danmus id, sorted by {@code displaytime}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>
     *     {@code timeStart} and/or {@code timeEnd} is invalid ({@code timeStart} <= {@code timeEnd} // done
     *     or any of them < 0 or > video duration) // done
     *   </li>
     * <li>the video is not published</li> //done
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter)
    {
        if (bv == null || timeStart < 0 || timeEnd < 0 || timeStart > timeEnd)
        {
            return null;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT * FROM Video_base WHERE BvID = ?"
             )) {
            statement.setString(1, bv);
            ResultSet resultSet = statement.executeQuery();
            //判断是否存在视频
            if (resultSet.next())
            {
                //是否超过duration
                float duration = resultSet.getFloat("Duration");
                if(timeStart > duration || timeEnd > duration)
                {
                    return null;
                }
                //比较publictime和当前时间
                Timestamp publictime = resultSet.getTimestamp("PublicTime");
                if(publictime == null)
                {
                    return null;
                }
                if(publictime.after(Timestamp.valueOf(LocalDateTime.now())))
                {
                    return null;
                }

                //查询弹幕
                if(filter)
                {
//                    String sql1 = "SELECT * FROM Danmu_base WHERE BvID = ? AND danmu_base.displaytime >= ? AND danmu_base.displaytime <= ? AND DanmuID NOT IN " +
//                            "(SELECT danmuid FROM Danmu_base WHERE BvID = ? AND danmu_base.displaytime >= ? AND danmu_base.displaytime <= ? GROUP BY Content HAVING COUNT(*) > 1)";
                    String sql1 = "SELECT * FROM Danmu_base WHERE BvID = ? AND danmu_base.displaytime >= ? AND danmu_base.displaytime <= ? AND DanmuID NOT IN " +
                              "(SELECT danmuid FROM Danmu_base WHERE BvID = ? AND danmu_base.displaytime >= ? AND danmu_base.displaytime <= ? GROUP BY Content, DanmuID HAVING COUNT(*) > 1)";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                        stmt1.setString(1, bv);
                        stmt1.setFloat(2, timeStart);
                        stmt1.setFloat(3, timeEnd);
                        stmt1.setString(4, bv);
                        stmt1.setFloat(5, timeStart);
                        stmt1.setFloat(6, timeEnd);
                        ResultSet rs1 = stmt1.executeQuery();
                        if(!rs1.next())
                        {
                            return null;
                        }
                        else
                        {
                            Long[] danmu = new Long[rs1.getFetchSize()];
                            int i = 0;
                            while(rs1.next())
                            {
                                danmu[i] = rs1.getLong("DanmuID");
                                i++;
                            }
                            return Arrays.asList(danmu);
                        }
                    } catch (SQLException e) {
                        return null;
                    }

                }
                else
                {
                    String sql1 = "SELECT * FROM Danmu_base WHERE BvID = ? AND danmu_base.displaytime >= ? AND danmu_base.displaytime <= ?";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                        stmt1.setString(1, bv);
                        stmt1.setFloat(2, timeStart);
                        stmt1.setFloat(3, timeEnd);
                        ResultSet rs1 = stmt1.executeQuery();
                        if(!rs1.next())
                        {
                            return null;
                        }
                        else
                        {
                            Long[] danmu = new Long[rs1.getFetchSize()];
                            int j = 0;
                            while(rs1.next())
                            {
                                danmu[j] = rs1.getLong("DanmuID");
                                j++;
                            }
                            return Arrays.asList(danmu);
                        }
                    } catch (SQLException e) {
                        return null;
                    }
                }

            }
            else
            {
                return null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    /**
     * Likes a danmu.
     * If the user already liked the danmu, this operation will cancel the like status.
     * It is mandatory that the user shall watch the video first before he/she can like a danmu of it.
     *
     * @param auth the current user's authentication information
     * @param id   the danmu's id
     * @return the like state of the user to this danmu after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a danmu corresponding to the {@code id}</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    public boolean likeDanmu(AuthInfo auth, long id)
    {
        if (auth == null || !Authisvalid(auth) || id < 0)
        {
            return false;
        }
        //判断是否存在弹幕
        String sql = "SELECT * FROM Danmu_base WHERE DanmuID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            if(!rs.next())
            {
                return false;
            }
            else
            {
                //是否观看
                String sql1 = "SELECT * FROM video_viewer WHERE UserID = ? AND BvID = ?";
                try (Connection conn1 = dataSource.getConnection();
                     PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                    stmt1.setLong(1, auth.getMid());
                    stmt1.setString(2, rs.getString("BvID"));
                    ResultSet rs1 = stmt1.executeQuery();
                    if(!rs1.next())
                    {
                        return false;
                    }
                    else
                    {
                        //判断是否已经点赞
                        String sql2 = "SELECT * FROM Danmu_liked WHERE UserID = ? AND DanmuID = ?";
                        try (Connection conn2 = dataSource.getConnection();
                             PreparedStatement stmt2 = conn2.prepareStatement(sql2)) {
                            stmt2.setLong(1, auth.getMid());
                            stmt2.setLong(2, id);
                            ResultSet rs2 = stmt2.executeQuery();
                            if(!rs2.next())
                            {
                                //点赞
                                String sql3 = "INSERT INTO danmu_liked (UserID, DanmuID) VALUES (?, ?)";
                                try (Connection conn3 = dataSource.getConnection();
                                     PreparedStatement stmt3 = conn3.prepareStatement(sql3)) {
                                    stmt3.setLong(1, auth.getMid());
                                    stmt3.setLong(2, id);
                                    stmt3.executeUpdate();
                                    return true;
                                } catch (SQLException e) {
                                    return false;
                                }
                            }
                            else
                            {
                                //取消点赞
                                String sql4 = "DELETE FROM danmu_liked WHERE UserID = ? AND DanmuID = ?";
                                try (Connection conn4 = dataSource.getConnection();
                                     PreparedStatement stmt4 = conn4.prepareStatement(sql4)) {
                                    stmt4.setLong(1, auth.getMid());
                                    stmt4.setLong(2, id);
                                    stmt4.executeUpdate();
                                    return false;
                                } catch (SQLException e) {
                                    return false;
                                }
                            }
                        } catch (SQLException e) {
                            return false;
                        }
                    }
                } catch (SQLException e) {
                    return false;
                }
            }
        } catch (SQLException e) {
            return false;
        }
    }
}
