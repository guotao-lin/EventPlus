package db.mongodb;

//This line needs manual import.
import static com.mongodb.client.model.Filters.eq;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import db.DBConnection;
import entity.Item;
import entity.Item.ItemBuilder;
import external.ExternalAPI;
import external.ExternalAPIFactory;

public class MongoDBConnection implements DBConnection {
	private MongoClient mongoClient;
	private MongoDatabase db;

	public MongoDBConnection() {
		// Connects to local mongodb server.
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase(MongoDBUtil.DB_NAME);
	}

	@Override
	public void close() {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}

	@Override
	public void setFavoriteItems(String userId, List<String> itemIds) {
		db.getCollection("users").updateOne(new Document("user_id", userId),
				new Document("$push", new Document("favorite", new Document("$each", itemIds))));
	}

	@Override
	public void unsetFavoriteItems(String userId, List<String> itemIds) {
		db.getCollection("users").updateOne(new Document("user_id", userId), 
				new Document("$pullAll", new Document("favorite", itemIds)));
	}

	@Override
	public Set<String> getFavoriteItemIds(String userId) {
		Set<String> favoriteItems = new HashSet<String>();
		FindIterable<Document> iterable = db.getCollection("users").find(eq("user_id", userId));
		if (iterable.first().containsKey("favorite")) {
			@SuppressWarnings("unchecked")
			List<String> list = (List<String>) iterable.first().get("favorite");
			favoriteItems.addAll(list);
		}
		return favoriteItems;
	}

	@Override
	public Set<Item> getFavoriteItems(String userId) {
		Set<String> itemIds = getFavoriteItemIds(userId);
		Set<Item> favoriteItems = new HashSet<>();
		for (String itemId : itemIds) {
			FindIterable<Document> iterable = db.getCollection("items").find(eq("item_id", itemId));
			Document rs = iterable.first();
			ItemBuilder builder = new ItemBuilder();

			// Because itemId is unique and given one item id there should
			// have
			// only one result returned.
			builder.setItemId(rs.getString("item_id"));
			builder.setName(rs.getString("name"));
			builder.setCity(rs.getString("city"));
			builder.setState(rs.getString("state"));
			builder.setCountry(rs.getString("country"));
			builder.setZipcode(rs.getString("zipcode"));
			builder.setRating(rs.getDouble("rating"));
			builder.setAddress(rs.getString("address"));
			builder.setLatitude(rs.getDouble("latitude"));
			builder.setLongitude(rs.getDouble("longitude"));
			builder.setDescription(rs.getString("description"));
			builder.setSnippet(rs.getString("snippet"));
			builder.setSnippetUrl(rs.getString("snippet_url"));
			builder.setImageUrl(rs.getString("image_url"));
			builder.setUrl(rs.getString("url"));
			
			Set<String> categories = getCategories(itemId);
			builder.setCategories(categories);
			favoriteItems.add(builder.build());
		}
		return favoriteItems;
	}

	@Override
	public Set<String> getCategories(String itemId) {
		Set<String> categories = new HashSet<>();
		FindIterable<Document> iterable = db.getCollection("items").find(eq("item_id", itemId));
		if (iterable.first().containsKey("categories")) {
			@SuppressWarnings("unchecked")
			List<String> list = (List<String>) iterable.first().get("categories");
			categories.addAll(list);
		}
		return categories;
	}

	@Override
	public List<Item> searchItems(String userId, double lat, double lon, String term) {
		// Connect to external API
		ExternalAPI api = ExternalAPIFactory.getExternalAPI(); // moved here
		List<Item> items = api.search(lat, lon, term);
		for (Item item : items) {
			// Save the item into our own db.
			saveItem(item);
		}
		return items;
	}

	@Override
	public void saveItem(Item item) {
		// You can construct the query like
		// db.getCollection("items").find(new Document().append("item_id",
		// item.getItemId()))
		// But the java drive provides you a clearer way to do this.

		FindIterable<Document> iterable = db.getCollection("items").find(eq("item_id", item.getItemId()));
		if (iterable.first() == null) {
			db.getCollection("items")
					.insertOne(new Document().append("item_id", item.getItemId()).append("name", item.getName())
							.append("city", item.getCity()).append("state", item.getState())
							.append("country", item.getCountry()).append("zip_code", item.getZipcode())
							.append("rating", item.getRating()).append("address", item.getAddress())
							.append("latitude", item.getLatitude()).append("longitude", item.getLongitude())
							.append("description", item.getDescription()).append("snippet", item.getSnippet())
							.append("snippet_url", item.getSnippetUrl()).append("image_url", item.getImageUrl())
							.append("url", item.getUrl()).append("categories", item.getCategories()));
		}
	}

	@Override
	public String getFullname(String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean verifyLogin(String userId, String password) {
		// TODO Auto-generated method stub
		return false;
	}

}
