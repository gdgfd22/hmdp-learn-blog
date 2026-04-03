package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SocialNotificationMessage;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.BlogComments;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogCommentsMapper;
import com.hmdp.mq.SocialNotificationProducer;
import com.hmdp.service.IBlogCommentsService;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.NotificationConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Service
public class BlogCommentsServiceImpl extends ServiceImpl<BlogCommentsMapper, BlogComments> implements IBlogCommentsService {

    @Resource
    private IBlogService blogService;

    @Resource
    private IUserService userService;

    @Resource
    private SocialNotificationProducer socialNotificationProducer;

    @Override
    @Transactional
    public Result saveComment(BlogComments blogComments) {
        UserDTO currentUser = UserHolder.getUser();
        Blog blog = blogService.getById(blogComments.getBlogId());
        if (blog == null) {
            return Result.fail("Blog not found");
        }

        blogComments.setUserId(currentUser.getId());
        if (blogComments.getParentId() == null) {
            blogComments.setParentId(0L);
        }
        if (blogComments.getAnswerId() == null) {
            blogComments.setAnswerId(0L);
        }
        if (blogComments.getLiked() == null) {
            blogComments.setLiked(0);
        }
        if (blogComments.getStatus() == null) {
            blogComments.setStatus(Boolean.FALSE);
        }

        boolean success = save(blogComments);
        if (!success) {
            return Result.fail("Comment create failed");
        }

        blogService.update()
                .setSql("comments = IFNULL(comments, 0) + 1")
                .eq("id", blogComments.getBlogId())
                .update();

        Long receiverUserId = resolveReceiverUserId(blogComments, blog);
        if (receiverUserId != null && !receiverUserId.equals(currentUser.getId())) {
            SocialNotificationMessage message = new SocialNotificationMessage();
            message.setReceiverUserId(receiverUserId);
            message.setSenderUserId(currentUser.getId());
            message.setBizId(blogComments.getId());
            message.setType(NotificationConstants.TYPE_BLOG_COMMENT);
            message.setContent(buildNotificationContent(currentUser, blogComments.getContent()));
            message.setEventTime(LocalDateTime.now());
            socialNotificationProducer.publishAfterCommit(message);
        }
        return Result.ok(blogComments.getId());
    }

    @Override
    public Result queryCommentsByBlogId(Long blogId) {
        List<BlogComments> comments = query()
                .eq("blog_id", blogId)
                .orderByDesc("create_time")
                .list();
        if (comments.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        for (BlogComments comment : comments) {
            User user = userService.getById(comment.getUserId());
            if (user != null) {
                comment.setName(user.getNickName());
                comment.setIcon(user.getIcon());
            }
        }
        return Result.ok(comments);
    }

    private Long resolveReceiverUserId(BlogComments blogComments, Blog blog) {
        if (blogComments.getAnswerId() != null && blogComments.getAnswerId() > 0) {
            BlogComments answerComment = getById(blogComments.getAnswerId());
            if (answerComment != null) {
                return answerComment.getUserId();
            }
        }
        return blog.getUserId();
    }

    private String buildNotificationContent(UserDTO currentUser, String content) {
        String displayContent = content == null ? "" : content.trim();
        if (displayContent.length() > 40) {
            displayContent = displayContent.substring(0, 40) + "...";
        }
        return currentUser.getNickName() + " commented: " + displayContent;
    }
}