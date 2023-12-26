package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import javax.swing.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import java.util.HashMap;
import java.util.Map;


/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class VideoServiceImpl implements VideoService {

/**
 * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
 * <p>
 * Marking a field with {@link Autowired} annotation enables our framework to automatically
 * provide you a well-configured instance of {@link DataSource}.
 * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
 */
@Autowired
private DataSource dataSource;


    public boolean isvalidvideo(PostVideoReq req)
    {
        return req != null && req.getTitle() != null && !req.getTitle().isEmpty() && !(req.getDuration() < 10) && !req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now()));
    }

    public boolean isSuperuser(AuthInfo auth)
    {
        if(auth == null || auth.getMid() < 0)
        {
            return false;
        }
        String sql = "SELECT * FROM User_base WHERE UserID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if(rs.next())
            {
                return String.valueOf(rs.getInt(8)).equals("SUPERUSER");
            }
            else
            {
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
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
     * Posts a video. Its commit time shall be {@link LocalDateTime#now()}.
     *
     * @param auth the current user's authentication information
     * @param req  the video's information
     * @return the video's {@code bv}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>{@code req} is invalid
     *     <ul>
     *       <li>{@code title} is null or empty</li> //done
     *       <li>there is another video with same {@code title} and same user</li> //done
     *       <li>{@code duration} is less than 10 (so that no chunk can be divided)</li> //done
     *       <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li> //done
     *     </ul>
     *   </li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req)
    {
        if (auth == null || !Authisvalid(auth) || req == null || req.getTitle() == null || req.getTitle().isEmpty() || req.getDuration() < 10 || req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now())))
        {
            return null;
        }
        String sql = "SELECT * FROM Video_base WHERE Title = ? AND OwnerID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, req.getTitle());
            stmt.setLong(2, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if(rs.next())
            {
                return null;
            }
            else
            {
                String sql1 = "INSERT INTO Video_base (Title, OwnerID, CommitTime, PublicTime, Duration) VALUES (?, ?, ?, ?, ?)";
                try (Connection conn1 = dataSource.getConnection();
                     PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                    stmt1.setString(1, req.getTitle());
                    stmt1.setLong(2, auth.getMid());
                    stmt1.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
                    stmt1.setTimestamp(4, req.getPublicTime());
                    stmt1.setFloat(5, req.getDuration());
                    stmt1.executeUpdate();
                    return req.getTitle();
                } catch (SQLException e) {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a video.
     * This operation can be performed by the video owner or a superuser.
     * The coins of this video will not be returned to their donators.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return success or not
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>{@code auth} is not the owner of the video nor a superuser</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean deleteVideo(AuthInfo auth, String bv)
    {
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty())
        {
            return false;
        }
        String sql = "SELECT * FROM Video_base WHERE BvID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if(rs.next())
            {
                if(rs.getLong("OwnerID") == auth.getMid() || isSuperuser(auth))
                {
                    String sql1 = "DELETE FROM Video_base WHERE BvID = ?";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                        stmt1.setString(1, bv);
                        stmt1.executeUpdate();
                        return true;
                    } catch (SQLException e) {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
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
     * Updates the video's information.
     * Only the owner of the video can update the video's information.
     * If the video was reviewed before, a new review for the updated video is required.
     * The duration shall not be modified and therefore the likes, favorites and danmus are not required to update.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @param req  the new video information
     * @return {@code true} if the video needs to be re-reviewed (was reviewed before), {@code false} otherwise
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>{@code auth} is not the owner of the video</li> //done
     *   <li>{@code req} is invalid, as stated in {@link io.sustc.service.VideoService#postVideo(AuthInfo, PostVideoReq)}</li> //done
     *   <li>{@code duration} in {@code req} is changed compared to current one</li> //done
     *   <li>{@code req} is not changed compared to current information</li> //done
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req)
    {
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty() || req == null || req.getTitle() == null || req.getTitle().isEmpty() || req.getDuration() < 10 || req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now())))
        {
            return false;
        }
        //是否存在这个视频
        String sql = "SELECT * FROM Video_base WHERE BvID = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            if(rs.next())
            {
                //是否是这个视频的主人以及视频合法否
                if(rs.getLong("OwnerID") == auth.getMid() && isvalidvideo(req))
                {
                    //duration是否改变
                    if(rs.getFloat("Duration") == req.getDuration())
                    {
                        //是否与原来的信息相同
                        if(rs.getString("Title").equals(req.getTitle()) && rs.getTimestamp("PublicTime").equals(req.getPublicTime()) && rs.getString("Description").equals(req.getDescription()))
                        {
                            return false;
                        }
                        else
                        {
                            //是否需要重新审核
                            if(rs.getTimestamp("ReviewTime") != null)
                            {
                                String sql1 = "UPDATE Video_base SET Title = ?, PublicTime = ?, Description = ?, Reviewer = ?, ReviewTime = ? WHERE BvID = ?";
                                try (Connection conn1 = dataSource.getConnection();
                                     PreparedStatement stmt1 = conn1.prepareStatement(sql1)) {
                                    stmt1.setString(1, req.getTitle());
                                    stmt1.setTimestamp(2, req.getPublicTime());
                                    stmt1.setString(3, req.getDescription());
                                    stmt1.setNull(4, Types.BIGINT);
                                    stmt1.setNull(5, Types.TIMESTAMP);
                                    stmt1.setString(6, bv);
                                    stmt1.executeUpdate();
                                    return true;
                                } catch (SQLException e) {
                                    return false;
                                }
                            }
                            else
                            {
                                return false;
                            }
                        }

                    }
                    else
                    {
                        return false;
                    }

                }
                else
                {
                    return false;
                }
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
     * Search the videos by keywords (split by space).
     * You should try to match the keywords case-insensitively in the following fields:
     * <ol>
     *   <li>title</li>
     *   <li>description</li>
     *   <li>owner name</li>
     * </ol>
     * <p>
     * Sort the results by the relevance (sum up the number of keywords matched in the three fields).
     * <ul>
     *   <li>If a keyword occurs multiple times, it should be counted more than once.</li>
     *   <li>
     *     A character in these fields can only be counted once for each keyword
     *     but can be counted for different keywords.
     *   </li>
     *   <li>If two videos have the same relevance, sort them by the number of views.</li>
     * </u
     * <p>
     * Examples:
     * <ol>
     *   <li>
     *     If the title is "1122" and the keywords are "11 12",
     *     then the relevance in the title is 2 (one for "11" and one for "12").
     *   </li>
     *   <li>
     *     If the title is "111" and the keyword is "11",
     *     then the relevance in the title is 1 (one for the occurrence of "11").
     *   </li>
     *   <li>
     *     Consider a video with title "Java Tutorial", description "Basic to Advanced Java", owner name "John Doe".
     *     If the search keywords are "Java Advanced",
     *     then the relevance is 3 (one occurrence in the title and two in the description).
     *   </li>
     * </ol>
     * <p>
     * Unreviewed or unpublished videos are only visible to superusers or the video owner.
     *
     * @param auth     the current user's authentication information
     * @param keywords the keywords to search, e.g. "sustech database final review"
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s
     * @implNote If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code keywords} is null or empty</li>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */



    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum)
    {
        class node
        {
            String bv;
            int relevance;
            int views;
            int owner;
            int reviewedorpublished;
            public node(String bv, int relevance, int views, int owner, int reviewedorpublished)
            {
                this.bv = bv;
                this.relevance = relevance;
                this.views = views;
                this.owner = owner;
                this.reviewedorpublished = reviewedorpublished;
            }
        }

        if (auth == null || !Authisvalid(auth) || keywords == null || keywords.isEmpty() || pageSize <= 0 || pageNum <= 0)
        {
            return null;
        }
        String[] keyword = keywords.split(" ");
        //先查询有多少个视频
        int videonum = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT COUNT(*) FROM Video_base"
             )) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                {
                    //获取视频总数量
                    videonum = resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {

        }
        node [] nodes = new node[videonum];
        int cnt=-1;
        // 对每个keyword都进行检测出现次数，然后在nodes中views+1,
        // 同时使用哈希表映射bv和对应的nodes节点编号
        // 如果keyword在title中出现，relevance+1
        // 如果keyword在description中出现，relevance+1
        // 如果keyword在ownername中出现，relevance+1
        Map<String, Integer> hashMap = new HashMap<>();
        // Unreviewed or unpublished videos are only visible to superusers or the video owner.
        for (int i = 0; i < keyword.length; i++)
        {
            // 处理 title
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(
                         "SELECT * FROM Video_base WHERE Title ILIKE ?")) {
                statement.setString(1, "%" + keyword[i] + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String bvID = resultSet.getString("BvID");
                        if (!hashMap.containsKey(bvID)) {
                            cnt++;
                            hashMap.put(bvID, cnt);

                            int pd=1;
                            if(resultSet.getTimestamp("PublicTime") == null || resultSet.getTimestamp("ReviewTime") == null)
                            {
                                pd = 0;
                            }
                            else if(resultSet.getTimestamp("PublicTime").before(Timestamp.valueOf(LocalDateTime.now()))) //如果发布时间在当前时间之前
                            {
                                pd = 0;
                            }
                            else if(resultSet.getTimestamp("ReviewTime").before(Timestamp.valueOf(LocalDateTime.now()))) //如果审核时间在当前时间之前
                            {
                                pd = 0;
                            }
                            nodes[cnt] = new node(bvID, 1, resultSet.getInt("Views"), resultSet.getInt("OwnerID"), pd);
                        } else {
                            int nodeIndex = hashMap.get(bvID);
                            nodes[nodeIndex].relevance++;
                        }
                    }
                }
            } catch (SQLException e) {
                log.error("Failed to search video", e);
            }
            // 处理 description
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(
                         "SELECT * FROM Video_base WHERE description ILIKE ?")) {
                statement.setString(1, "%" + keyword[i] + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String bvID = resultSet.getString("BvID");
                        if (!hashMap.containsKey(bvID)) {
                            cnt++;
                            hashMap.put(bvID, cnt);

                            int pd=1;
                            if(resultSet.getTimestamp("PublicTime") == null || resultSet.getTimestamp("ReviewTime") == null)
                            {
                                pd = 0;
                            }
                            else if(resultSet.getTimestamp("PublicTime").before(Timestamp.valueOf(LocalDateTime.now()))) //如果发布时间在当前时间之前
                            {
                                pd = 0;
                            }
                            else if(resultSet.getTimestamp("ReviewTime").before(Timestamp.valueOf(LocalDateTime.now()))) //如果审核时间在当前时间之前
                            {
                                pd = 0;
                            }
                            nodes[cnt] = new node(bvID, 1, resultSet.getInt("Views"), resultSet.getInt("OwnerID"), pd);
                        } else {
                            int nodeIndex = hashMap.get(bvID);
                            nodes[nodeIndex].relevance++;
                        }
                    }
                }
            } catch (SQLException e) {
                log.error("Failed to search video", e);
            }
            // 处理 ownername
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(
                         "SELECT * FROM Video_base WHERE ownername ILIKE ?")) {
                statement.setString(1, "%" + keyword[i] + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String bvID = resultSet.getString("BvID");
                        if (!hashMap.containsKey(bvID)) {
                            cnt++;
                            hashMap.put(bvID, cnt);

                            int pd=1;
                            if(resultSet.getTimestamp("PublicTime") == null || resultSet.getTimestamp("ReviewTime") == null)
                            {
                                pd = 0;
                            }
                            else if(resultSet.getTimestamp("PublicTime").before(Timestamp.valueOf(LocalDateTime.now()))) //如果发布时间在当前时间之前
                            {
                                pd = 0;
                            }
                            else if(resultSet.getTimestamp("ReviewTime").before(Timestamp.valueOf(LocalDateTime.now()))) //如果审核时间在当前时间之前
                            {
                                pd = 0;
                            }
                            nodes[cnt] = new node(bvID, 1, resultSet.getInt("Views"), resultSet.getInt("OwnerID"), pd);
                        } else {
                            int nodeIndex = hashMap.get(bvID);
                            nodes[nodeIndex].relevance++;
                        }
                    }
                }
            } catch (SQLException e) {
                log.error("Failed to search video", e);
            }
        }
        int cnt1 = -1;
        node [] nodes1 = new node[cnt+1];
        //筛选可视视频
        for (int i = 0; i < nodes.length; i++)
        {
            if(nodes[i].reviewedorpublished == 0)
            {
                if(!isSuperuser(auth) && nodes[i].owner != auth.getMid())
                {

                }
                else
                {
                    cnt1++;
                    nodes1[cnt1] = nodes[i];
                }
            }
            else
            {
                cnt1++;
                nodes1[cnt1] = nodes[i];
            }
        }
        // 对nodes1进行排序,先按照relevance排序，再按照views排序
        Arrays.sort(nodes1, (o1, o2) -> {
            if(o1.relevance == o2.relevance)
            {
                return o1.views - o2.views;
            }
            else
            {
                return o1.relevance - o2.relevance;
            }
        });

        // 获取对应页的视频
        List<String> ans = null;
        for (int i = (pageNum - 1) * pageSize; i < pageNum * pageSize && i < cnt1; i++)
        {
            ans.add(nodes1[i].bv);
        }
        return ans;
    }

    /**
     * Calculates the average view rate of a video.
     * The view rate is defined as the user's view time divided by the video's duration.
     *
     * @param bv the video's {@code bv}
     * @return the average view rate
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>no one has watched this video</li> //done
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    @Override
    public double getAverageViewRate(String bv)
    {
        if (bv == null || bv.isEmpty())
        {
            return -1;
        }
       //先查有没有这个视频
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT * FROM Video_base WHERE BvID = ?"
             )) {
            statement.setString(1, bv);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                {
                    //再查有没有人看过这个视频
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement statement1 = conn1.prepareStatement(
                                 "SELECT * FROM Video_viewer WHERE BvID = ?"
                         )) {
                        statement1.setString(1, bv);
                        try (ResultSet resultSet1 = statement1.executeQuery()) {
                            if (resultSet1.next()) {
                                //获取所有看过这个视频的人的观看时间的平均值
                                try (Connection conn2 = dataSource.getConnection();
                                     PreparedStatement statement2 = conn2.prepareStatement(
                                             "SELECT AVG(Time) FROM Video_viewer WHERE BvID = ?"
                                     )) {
                                    statement2.setString(1, bv);
                                    try (ResultSet resultSet2 = statement2.executeQuery()) {
                                        if (resultSet2.next()) {
                                            return resultSet2.getDouble(1) / resultSet.getFloat("Duration");
                                        }
                                    }

                                } catch (SQLException e) {
                                    log.error("Failed to get average view rate", e);
                                }
                            }
                        }
                    } catch (SQLException e) {
                        log.error("Failed to get average view rate", e);
                    }
                }
                else
                {
                    return -1;
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get average view rate", e);
        }
        return -1;
    }

    /**
     * Gets the hotspot of a video.
     * With splitting the video into 10-second chunks, hotspots are defined as chunks with the most danmus.
     *
     * @param bv the video's {@code bv}
     * @return the index of hotspot chunks (start from 0)
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>no one has sent danmu on this video</li> //done
     * </ul>
     * If any of the corner case happened, an empty set shall be returned.
     */
    @Override
    public Set<Integer> getHotspot(String bv)
    {
        if (bv == null || bv.isEmpty())
        {
            return null;
        }
        //先查有无这个视频
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT * FROM Video_base WHERE BvID = ?"
             )) {
            statement.setString(1, bv);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                {
                    //再查有无人发过弹幕
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement statement1 = conn1.prepareStatement(
                                 "SELECT * FROM Danmu_base WHERE BvID = ?"
                         )) {
                        statement1.setString(1, bv);
                        try (ResultSet resultSet1 = statement1.executeQuery()) {
                            if (resultSet1.next()) {
                                int[] cnt = new int[(int) resultSet.getFloat("Duration") / 10 + 1];
                                //获取所有弹幕的时间
                                try (Connection conn2 = dataSource.getConnection();
                                     PreparedStatement statement2 = conn2.prepareStatement(
                                             "SELECT * FROM Danmu_base WHERE BvID = ?"
                                     )) {
                                    statement2.setString(1, bv);
                                    try (ResultSet resultSet2 = statement2.executeQuery()) {
                                        while (resultSet2.next()) {
                                            cnt[(int) resultSet2.getFloat("Displaytime") / 10]++;
                                        }
                                        int max = 0;
                                        for (int i = 0; i < cnt.length; i++)
                                        {
                                            if(cnt[i] > cnt[max])
                                            {
                                                max = i;
                                            }
                                        }
                                        //返回所有最大值一样的下标
                                        Set<Integer> ans = null;
                                        for (int i = 0; i < cnt.length; i++)
                                        {
                                            if(cnt[i] == cnt[max])
                                            {
                                                ans.add(i);
                                            }
                                        }
                                        return ans;
                                    }
                                } catch (SQLException e) {
                                    log.error("Failed to get hotspot", e);
                                }
                            }
                        } catch (SQLException e) {
                            log.error("Failed to get hotspot", e);
                        }
                    } catch (SQLException e) {
                        log.error("Failed to get hotspot", e);
                    }
                }
                else
                {
                    return null;
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get hotspot", e);
        }
        return null;
    }

    /**
     * Reviews a video by a superuser.
     * If the video is already reviewed, do not modify the review info.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return {@code true} if the video is newly successfully reviewed, {@code false} otherwise
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>{@code auth} is not a superuser or he/she is the owner</li> //done
     *   <li>the video is already reviewed</li> //done
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean reviewVideo(AuthInfo auth, String bv)
    {
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty())
        {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT OwnerID FROM Video_base WHERE BvID = ?"
             )) {
            statement.setString(1, bv);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    //判断是否是超级用户
                    if (!isSuperuser(auth)) {
                        return false;
                    }
                    //如果不是这个视频的主人
                    if (resultSet.getLong("OwnerID") != auth.getMid()) {
                        //判断是否已经审核过
                        PreparedStatement statement1 = conn.prepareStatement(
                                "SELECT * FROM Video_base WHERE BvID = ? AND Reviewer IS NULL"
                        );
                        statement1.setString(1, bv);
                        ResultSet resultSet1 = statement1.executeQuery();
                        if (resultSet1.next()) {
                            PreparedStatement statement2 = conn.prepareStatement(
                                    "UPDATE Video_base SET Reviewer = ?, ReviewTime = ? WHERE BvID = ?"
                            );
                            statement2.setLong(1, auth.getMid());
                            statement2.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                            statement2.setString(3, bv);
                            statement2.executeUpdate();
                            return true;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to review video", e);
        }
        return false;
    }

    /**
     * Donates one coin to the video. A user can at most donate one coin to a video. //done
     * The user can only coin a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}). //done
     * It is not mandatory that the user shall watch the video first before he/she donates coin to it. //done
     * If the current user donated a coin to this video successfully, he/she's coin number shall be reduced by 1. //done
     * However, the coin number of the owner of the video shall NOT increase.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return whether a coin is successfully donated
     * @implNote There is not way to earn coins in this project for simplicity
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>the user cannot search this video or he/she is the owner</li> //done
     *   <li>the user has no coin or has donated a coin to this video (user cannot withdraw coin donation)</li> //done
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean coinVideo(AuthInfo auth, String bv)
    {
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty())
        {
            return false;
        }
        //先查询有多少个视频
        int videonum = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT COUNT(*) FROM Video_base"
             )) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                {
                    //获取视频总数量
                    videonum = resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {
        }
        //先判断该用户能否搜索到这个视频
        if(searchVideo(auth, bv, videonum,1).contains(bv))
        {
            //再判断该用户是否是这个视频的主人
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(
                         "SELECT OwnerID FROM Video_base WHERE BvID = ?"
                 )) {
                statement.setString(1, bv);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        if (resultSet.getLong("OwnerID") != auth.getMid()) {
                            //再判断该用户是否已经捐过币了
                            try (Connection conn1 = dataSource.getConnection();
                                 PreparedStatement statement1 = conn1.prepareStatement(
                                         "SELECT * FROM Video_coin WHERE UserID = ? AND BvID = ?"
                                 )) {
                                statement1.setLong(1, auth.getMid());
                                statement1.setString(2, bv);
                                try (ResultSet resultSet1 = statement1.executeQuery()) {
                                    if (resultSet1.next()) {
                                        return false;
                                    } else {
                                        //再判断该用户是否有币
                                        try (Connection conn2 = dataSource.getConnection();
                                             PreparedStatement statement2 = conn2.prepareStatement(
                                                     "SELECT * FROM User_base WHERE UserID = ?"
                                             )) {
                                            statement2.setLong(1, auth.getMid());
                                            try (ResultSet resultSet2 = statement2.executeQuery()) {
                                                if (resultSet2.next()) {
                                                    if (resultSet2.getInt("Coins") > 0) {
                                                        //捐币
                                                        try (Connection conn3 = dataSource.getConnection();
                                                             PreparedStatement statement3 = conn3.prepareStatement(
                                                                     "INSERT INTO Video_coin (UserID, BvID) VALUES (?, ?)"
                                                             )) {
                                                            statement3.setLong(1, auth.getMid());
                                                            statement3.setString(2, bv);
                                                            statement3.executeUpdate();
                                                            //减少用户的币
                                                            try (Connection conn4 = dataSource.getConnection();
                                                                 PreparedStatement statement4 = conn4.prepareStatement(
                                                                         "UPDATE User_base SET Coin = ? WHERE UserID = ?"
                                                                 )) {
                                                                statement4.setInt(1, resultSet2.getInt("Coins") - 1);
                                                                statement4.setLong(2, auth.getMid());
                                                                statement4.executeUpdate();
                                                                return true;
                                                            }
                                                        }
                                                    } else {
                                                        return false;
                                                    }
                                                }
                                            }
                                        } catch (SQLException e) {
                                            log.error("Failed to coin video", e);
                                        }
                                    }
                                }
                            } catch (SQLException e) {
                                log.error("Failed to coin video", e);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    /**
     * Likes a video.
     * The user can only like a video if he/she can search it ({@link io.sustc.service.VideoService#searchVideo(AuthInfo, String, int, int)}).  //done
     * If the user already liked the video, the operation will cancel the like.
     * It is not mandatory that the user shall watch the video first before he/she likes to it. //done
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return the like state of the user to this video after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>the user cannot search this video or the user is the video owner</li> //done
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean likeVideo(AuthInfo auth, String bv)
    {
        //仿照coinVideo来编写
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty())
        {
            return false;
        }
        //先查询有多少个视频
        int videonum = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT COUNT(*) FROM Video_base"
             )) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                {
                    //获取视频总数量
                    videonum = resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {
        }
        //先判断该用户能否搜索到这个视频
        if(searchVideo(auth, bv, videonum,1).contains(bv))
        {
            //再判断该用户是否是这个视频的主人，如果是主人则不能点赞
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(
                         "SELECT OwnerID FROM Video_base WHERE BvID = ?"
                 )) {
                statement.setString(1, bv);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        if (resultSet.getLong("OwnerID") != auth.getMid()) {
                            //再判断该用户是否已经点过赞了
                            try (Connection conn1 = dataSource.getConnection();
                                 PreparedStatement statement1 = conn1.prepareStatement(
                                         "SELECT * FROM Video_like WHERE UserID = ? AND BvID = ?"
                                 )) {
                                statement1.setLong(1, auth.getMid());
                                statement1.setString(2, bv);
                                try (ResultSet resultSet1 = statement1.executeQuery()) {
                                    if (resultSet1.next()) {
                                        //如果点过赞了，则取消点赞
                                        try (Connection conn2 = dataSource.getConnection();
                                             PreparedStatement statement2 = conn2.prepareStatement(
                                                     "DELETE FROM Video_like WHERE UserID = ? AND BvID = ?"
                                             )) {
                                            statement2.setLong(1, auth.getMid());
                                            statement2.setString(2, bv);
                                            statement2.executeUpdate();
                                            return false;
                                        }
                                    } else {
                                        //如果没点过赞，则点赞
                                        try (Connection conn3 = dataSource.getConnection();
                                             PreparedStatement statement3 = conn3.prepareStatement(
                                                     "INSERT INTO Video_like (UserID, BvID) VALUES (?, ?)"
                                             )) {
                                            statement3.setLong(1, auth.getMid());
                                            statement3.setString(2, bv);
                                            statement3.executeUpdate();
                                            return true;
                                        }
                                    }
                                }
                            } catch (SQLException e) {
                                log.error("Failed to like video", e);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    /**
     * Collects a video.
     * The user can only collect a video if he/she can search it. //done
     * If the user already collected the video, the operation will cancel the collection.
     * It is not mandatory that the user shall watch the video first before he/she collects coin to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return the collect state of the user to this video after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li> //done
     *   <li>cannot find a video corresponding to the {@code bv}</li> //done
     *   <li>the user cannot search this video or the user is the video owner</li> //done
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean collectVideo(AuthInfo auth, String bv)
    {
        //仿照coinVideo来编写
        if (auth == null || !Authisvalid(auth) || bv == null || bv.isEmpty())
        {
            return false;
        }
        //先查询有多少个视频
        int videonum = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(
                     "SELECT COUNT(*) FROM Video_base"
             )) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                {
                    //获取视频总数量
                    videonum = resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {
        }
        //先判断该用户能否搜索到这个视频
        if(searchVideo(auth, bv, videonum,1).contains(bv))
        {
            //再判断该用户是否是这个视频的主人，如果是主人则不能收藏
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(
                         "SELECT OwnerID FROM Video_base WHERE BvID = ?"
                 )) {
                statement.setString(1, bv);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        if (resultSet.getLong("OwnerID") != auth.getMid()) {
                            //再判断该用户是否已经收藏过了
                            try (Connection conn1 = dataSource.getConnection();
                                 PreparedStatement statement1 = conn1.prepareStatement(
                                         "SELECT * FROM video_favorite WHERE UserID = ? AND BvID = ?"
                                 )) {
                                statement1.setLong(1, auth.getMid());
                                statement1.setString(2, bv);
                                try (ResultSet resultSet1 = statement1.executeQuery()) {
                                    if (resultSet1.next()) {
                                        //如果收藏过了，则取消收藏
                                        try (Connection conn2 = dataSource.getConnection();
                                             PreparedStatement statement2 = conn2.prepareStatement(
                                                     "DELETE FROM video_favorite WHERE UserID = ? AND BvID = ?"
                                             )) {
                                            statement2.setLong(1, auth.getMid());
                                            statement2.setString(2, bv);
                                            statement2.executeUpdate();
                                            return false;
                                        }
                                    } else {
                                        //如果没收藏过，则收藏
                                        try (Connection conn3 = dataSource.getConnection();
                                             PreparedStatement statement3 = conn3.prepareStatement(
                                                     "INSERT INTO video_favorite (UserID, BvID) VALUES (?, ?)"
                                             )) {
                                            statement3.setLong(1, auth.getMid());
                                            statement3.setString(2, bv);
                                            statement3.executeUpdate();
                                            return true;
                                        }
                                    }
                                }
                            } catch (SQLException e) {
                                log.error("Failed to collect video", e);
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }
}
