package cis761.entity;

import java.util.Date;

public class Movies {
    String title;
    double budget = 0;
    String overview;
    Date releaseDate;
    double revenue = 0;
    int runTime;
    int genreId = 0;
    String languageCode = null;
    

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getBudget() {
        return this.budget;
    }

    public void setBudget(double budget) {
        this.budget = budget;
    }

    public String getOverview() {
        return this.overview;
    }

    public void setOverview(String overview) {
        this.overview = overview;
    }

    public Date getReleaseDate() {
        return this.releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public double getRevenue() {
        return this.revenue;
    }

    public void setRevenue(double revenue) {
        this.revenue = revenue;
    }

    public int getRunTime() {
        return this.runTime;
    }

    public void setRunTime(int runTime) {
        this.runTime = runTime;
    }

    public int getGenreId() {
        return this.genreId;
    }

    public void setGenreId(int genreId) {
        this.genreId = genreId;
    }

    public String getLanguageCode() {
        return this.languageCode;
    }

    public void setLanguageCode(String languageCode) {
        this.languageCode = languageCode;
    }
}
