package upf.edu.model;

import com.google.gson.Gson;

public final class HashTagCount {
  protected static Gson gson = new Gson();

  public final String hashTag;
  public final String lang;
  public final Long count;

  public HashTagCount(String hashTag, String lang, Long count) {
    this.hashTag = hashTag;
    this.lang = lang;
    this.count = count;
  }

  public long getCount(){
    return count;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }

}
