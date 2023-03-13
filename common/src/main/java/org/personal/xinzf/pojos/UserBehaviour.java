package org.personal.xinzf.pojos;

public class UserBehaviour {
    private Integer userId;
    private Integer itemId;
    private Integer categoryId;
    private String bahavior;
    private Integer timestamp;

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getItemId() {
        return itemId;
    }

    public void setItemId(Integer itemId) {
        this.itemId = itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBahavior() {
        return bahavior;
    }

    public void setBahavior(String bahavior) {
        this.bahavior = bahavior;
    }

    public Integer getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Integer timestamp) {
        this.timestamp = timestamp;
    }
}
