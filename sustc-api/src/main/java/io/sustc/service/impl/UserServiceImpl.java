package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
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
public class UserServiceImpl implements UserService {

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
                    String sql1 = "SELECT * FROM user_base WHERE UserID = ? AND QQ = ?";
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
     * Registers a new user.
     * {@code password} is a mandatory field, while {@code qq} and {@code wechat} are optional
     * <a href="https://openid.net/developers/how-connect-works/">OIDC</a> fields.
     *
     * @param req information of the new user
     * @return the new user's {@code mid}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code password} or {@code name} or {@code sex} in {@code req} is null or empty</li> //done
     *   <li>{@code birthday} in {@code req} is valid (not null nor empty) while it's not a birthday (X月X日)</li> //done
     *   <li>there is another user with same {@code qq} or {@code wechat} in {@code req}</li> //done
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    @Override
    public long register(RegisterUserReq req)
    {
        //
        if(req.getPassword() == null || req.getName() == null || req.getSex() == null || req.getPassword().isEmpty() || req.getName().isEmpty() || req.getSex().toString().isEmpty())
        {
            return -1;
        }
        if(req.getBirthday() != null && !req.getBirthday().isEmpty())
        {
            String[] birthday = req.getBirthday().split("月");
            if(birthday.length != 2)
            {
                return -1;
            }
            String[] day = birthday[1].split("日");
            if(day.length != 1)
            {
                return -1;
            }
            try
            {
                int month = Integer.parseInt(birthday[0]);
                int day1 = Integer.parseInt(day[0]);
                //根据月份判断日期是否合法
                if(month == 2)
                {
                    if(day1 < 1 || day1 > 29)
                    {
                        return -1;
                    }
                }
                else if(month == 4 || month == 6 || month == 9 || month == 11)
                {
                    if(day1 < 1 || day1 > 30)
                    {
                        return -1;
                    }
                }
                else
                {
                    if(day1 < 1 || day1 > 31)
                    {
                        return -1;
                    }
                }
            }
            catch (NumberFormatException e)
            {
                return -1;
            }
        }
        if(req.getQq() != null && !req.getQq().isEmpty()) {
            String sql = "SELECT * FROM User_base WHERE QQ = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getQq());
            } catch (SQLException e) {
                return -1;
            }
        }
        if(req.getWechat() != null && !req.getWechat().isEmpty()) {
            String sql = "SELECT * FROM User_base WHERE WeChat = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getWechat());
            } catch (SQLException e) {
                return -1;
            }
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "INSERT INTO User_base (Name, Sex, Birthday, Level, Coin, Sign, Identity, Password, QQ, WeChat) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                     + "RETURNING UserID"
             )) {
            short temp = 0;
            statement.setString(2, req.getName());
            statement.setString(3, req.getSex().toString());
            statement.setString(4, req.getBirthday());
            statement.setShort(5, temp);
            statement.setInt(6, temp);
            statement.setString(7, req.getSign());
            statement.setString(8, "USER");
            statement.setString(9, req.getPassword());
            if(req.getQq() == null || req.getQq().isEmpty())
            {
                statement.setNull(10, java.sql.Types.VARCHAR);
            }
            else
            {
                statement.setString(10, req.getQq());
            }
            if(req.getWechat() == null || req.getWechat().isEmpty())
            {
                statement.setNull(11, java.sql.Types.VARCHAR);
            }
            else {
                statement.setString(11, req.getWechat());
            }
            statement.executeUpdate();
            ResultSet rs = statement.getResultSet();
            return rs.getLong(1);
        } catch (SQLException e) {
            return -1;
        }
    }



    /**
     * Deletes a user.
     *
     * @param auth indicates the current user
     * @param mid  the user to be deleted
     * @return operation success or not
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a user corresponding to the {@code mid}</li> //done
     *   <li>the {@code auth} is invalid //done
     *     <ul>
     *       <li>both {@code qq} and {@code wechat} are non-empty while they do not correspond to same user</li> //done
     *       <li>{@code mid} is invalid while {@code qq} and {@code wechat} are both invalid (empty or not found)</li> //done
     *     </ul>
     *   </li>
     *   <li>the current user is a regular user while the {@code mid} is not his/hers</li>
     *   <li>the current user is a super user while the {@code mid} is neither a regular user's {@code mid} nor his/hers</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean deleteAccount(AuthInfo auth, long mid)
    {
        if(auth == null || !Authisvalid(auth)) //auth不合法
        {
            return false;
        }
        if(mid < 0) //mid不合法
        {
            return false;
        }
        //查找auth的信息
        String sql = "SELECT * FROM User_base WHERE UserID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if(!rs.next())
            {
                return false;
            }
            //判断auth的身份
            if(rs.getString(8).equals("USER")) //auth是普通用户
            {
                if(auth.getMid() != mid) //auth不是要删除的用户
                {
                    return false;
                }
            }
            else //auth是超级用户
            {
                if(mid != auth.getMid()) //mid不是auth的mid
                {
                    String sql1 = "SELECT * FROM User_base WHERE UserID = ?";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                        stmt1.setLong(1, mid);
                        ResultSet rs1 = stmt1.executeQuery();
                        if(!rs1.next())
                        {
                            return false;
                        }
                        else
                        {
                            //如果mid也是超级用户，则不能删除
                            if(rs1.getString(8).equals("SUPERUSER"))
                            {
                                return false;
                            }
                        }
                    } catch (SQLException e) {
                        return false;
                    }
                }
            }
        } catch (SQLException e) {
            return false;
        }
        //删除用户
        String sql2 = "DELETE FROM User_base WHERE UserID = ?";
        try (Connection conn2 = dataSource.getConnection();
             PreparedStatement stmt2 = conn2.prepareStatement(sql2)) {
            stmt2.setLong(1, mid);
            stmt2.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Follow the user with {@code mid}.
     * If that user has already been followed, unfollow the user.
     *
     * @param auth        the authentication information of the follower
     * @param followeeMid the user who will be followed
     * @return the follow state after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a user corresponding to the {@code followeeMid}</li> //done
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean follow(AuthInfo auth, long followeeMid)
    {
        if(auth == null || !Authisvalid(auth)) //auth不合法
        {
            return false;
        }
        if(followeeMid < 0) //followeeMid不合法
        {
            return false;
        }
        //不能关注自己
        if(auth.getMid() == followeeMid)
        {
            return false;
        }
        //auth合法
        //能否找到followeeMid
        String sql = "SELECT * FROM User_base WHERE UserID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, followeeMid);
            ResultSet rs = stmt.executeQuery();
            if(!rs.next())
            {
                return false;
            }
        } catch (SQLException e) {
            return false;
        }
        //关注或取消关注
        String sql1 = "SELECT * FROM User_follow WHERE upid = ? AND followerid = ?";
        try (Connection conn1 = dataSource.getConnection();
             PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
            stmt1.setLong(1, followeeMid);
            stmt1.setLong(2, auth.getMid());
            ResultSet rs1 = stmt1.executeQuery();
            if(rs1.next()) //已关注，取消关注
            {
                String sql2 = "DELETE FROM User_follow WHERE upid = ? AND followerid = ?";
                try (Connection conn2 = dataSource.getConnection();
                     PreparedStatement stmt2 = conn2.prepareStatement(sql2)) {
                    stmt2.setLong(1, followeeMid);
                    stmt2.setLong(2, auth.getMid());
                    stmt2.executeUpdate();
                    return false;
                } catch (SQLException e) {
                    return false;
                }
            }
            else //未关注，关注
            {
                String sql3 = "INSERT INTO User_follow (upid, followerid) VALUES (?, ?)";
                try (Connection conn3 = dataSource.getConnection();
                     PreparedStatement stmt3 = conn3.prepareStatement(sql3)) {
                    stmt3.setLong(1, followeeMid);
                    stmt3.setLong(2, auth.getMid());
                    stmt3.executeUpdate();
                    return true;
                } catch (SQLException e) {
                    return false;
                }
            }
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Gets the required information (in DTO) of a user.
     *
     * @param mid the user to be queried
     * @return the personal information of given {@code mid}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a user corresponding to the {@code mid}</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public UserInfoResp getUserInfo(long mid)
    {
        if(mid < 0)
        {
            return null;
        }

        UserInfoResp userInfoResp = new UserInfoResp();
        ResultSet rs_user_base;
        //查找用户基本信息
        String sql = "SELECT * FROM User_base WHERE UserID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);
            rs_user_base= stmt.executeQuery();
            if(!rs_user_base.next())
            {
                return null;
            }
        } catch (SQLException e) {
            return null;
        }
        userInfoResp.setMid(mid);
        try {
            userInfoResp.setCoin(rs_user_base.getInt(6));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //查找用户关注的人
        String sql1 = "SELECT * FROM User_follow WHERE followerid = ?";
        try (Connection conn1 = dataSource.getConnection();
             PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
            stmt1.setLong(1, mid);
            ResultSet rs1 = stmt1.executeQuery();
            ResultSet rs2=rs1;
            int i=0;
            while(rs1.next())
            {
                i++;
            }
            long [] newFollowing = new long[i];
            int j=0;
            while(rs2.next())
            {
                newFollowing[j] = rs1.getLong(1);
                j++;
            }
            userInfoResp.setFollowing(newFollowing);
        } catch (SQLException e) {
            return null;
        }
        //查找用户粉丝
        String sql2 = "SELECT * FROM User_follow WHERE upid = ?";
        try (Connection conn2 = dataSource.getConnection();
             PreparedStatement stmt2 = conn2.prepareStatement(sql2)) {
            stmt2.setLong(1, mid);
            ResultSet rs3 = stmt2.executeQuery();
            ResultSet rs4=rs3;
            int i=0;
            while(rs3.next())
            {
                i++;
            }
            long [] newFollower = new long[i];
            int j=0;
            while(rs4.next())
            {
                newFollower[j] = rs3.getLong(2);
                j++;
            }
            userInfoResp.setFollower(newFollower);
        } catch (SQLException e) {
            return null;
        }
        //查找用户观看过的视频
        String sql3 = "SELECT * FROM video_viewer WHERE UserID = ?";
        try (Connection conn3 = dataSource.getConnection();
             PreparedStatement stmt3 = conn3.prepareStatement(sql3)) {
            stmt3.setLong(1, mid);
            ResultSet rs5 = stmt3.executeQuery();
            ResultSet rs6=rs5;
            int i=0;
            while(rs5.next())
            {
                i++;
            }
            String [] newWatched = new String[i];
            int j=0;
            while(rs6.next())
            {
                newWatched[j] = rs5.getString(2);
                j++;
            }
            userInfoResp.setWatched(newWatched);
        } catch (SQLException e) {
            return null;
        }
        //查找用户点赞过的视频
        String sql4 = "SELECT * FROM video_like WHERE UserID = ?";
        try (Connection conn4 = dataSource.getConnection();
             PreparedStatement stmt4 = conn4.prepareStatement(sql4)) {
            stmt4.setLong(1, mid);
            ResultSet rs7 = stmt4.executeQuery();
            ResultSet rs8=rs7;
            int i=0;
            while(rs7.next())
            {
                i++;
            }
            String [] newLiked = new String[i];
            int j=0;
            while(rs8.next())
            {
                newLiked[j] = rs7.getString(2);
                j++;
            }
            userInfoResp.setLiked(newLiked);
        } catch (SQLException e) {
            return null;
        }
        //查找用户收藏过的视频
        String sql5 = "SELECT * FROM video_favorite WHERE UserID = ?";
        try (Connection conn5 = dataSource.getConnection();
             PreparedStatement stmt5 = conn5.prepareStatement(sql5)) {
            stmt5.setLong(1, mid);
            ResultSet rs9 = stmt5.executeQuery();
            ResultSet rs10=rs9;
            int i=0;
            while(rs9.next())
            {
                i++;
            }
            String [] newCollected = new String[i];
            int j=0;
            while(rs10.next())
            {
                newCollected[j] = rs9.getString(2);
                j++;
            }
            userInfoResp.setCollected(newCollected);
        } catch (SQLException e) {
            return null;
        }
        //查找用户发布过的视频
        String sql6 = "SELECT * FROM video_base WHERE ownerID = ?";
        try (Connection conn6 = dataSource.getConnection();
             PreparedStatement stmt6 = conn6.prepareStatement(sql6)) {
            stmt6.setLong(1, mid);
            ResultSet rs11 = stmt6.executeQuery();
            ResultSet rs12=rs11;
            int i=0;
            while(rs11.next())
            {
                i++;
            }
            String [] newPosted = new String[i];
            int j=0;
            while(rs12.next())
            {
                newPosted[j] = rs11.getString(1);
                j++;
            }
            userInfoResp.setPosted(newPosted);
        } catch (SQLException e) {
            return null;
        }
        return userInfoResp;
    }
}
