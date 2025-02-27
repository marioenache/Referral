package Me.Teenaapje.Referral.Database;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.entity.Player;

import Me.Teenaapje.Referral.ReferralCore;
import Me.Teenaapje.Referral.Utils.TopPlayer;
import Me.Teenaapje.Referral.Utils.Utils;

public class Database {

	public String host, database, username, password, table, parameters, dboption;
	public int port;
	private Connection connection;

	ReferralCore core = ReferralCore.core;

	public Database() {
		host = core.getConfig().getString("host");
		port = core.getConfig().getInt("port");
		database = core.getConfig().getString("database");
		username = core.getConfig().getString("username");
		password = core.getConfig().getString("password");
		table = core.getConfig().getString("table");
		parameters = core.getConfig().getString("databaseParameters");
		dboption = core.getConfig().getString("db");

		if (dboption.equalsIgnoreCase("local")) {
			sqlLiteSetup();
		} else if (dboption.equalsIgnoreCase("mysql")) {
			mysqlSetup();
		} else {
			core.getServer().getConsoleSender().sendMessage(ChatColor.DARK_RED + core.getDescription().getName() + " Incorrect selected database! Please use 'local' or 'mysql' in your config.");
			return;
		}

		createTable();
	}

	public void sqlLiteSetup() {
		File dir = core.getDataFolder();

		// look if directory exists and create
		if (!dir.exists()) {
			if (!dir.mkdir()) {
				core.getLogger().severe("Could not create directory for plugin: " + core.getDescription().getName());
				return;
			}
		}

		// check and create file if it doesn't exist
		File file = new File(dir, database + ".db");
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				core.getLogger().severe("Could not create database file: " + e.getMessage());
				e.printStackTrace();
				return;
			}
		}

		try {
			// Close any existing connection that might be invalid
			if (connection != null && !connection.isClosed()) {
				try {
					connection.close();
				} catch (SQLException e) {
					// Just log and continue
					core.getLogger().warning("Error closing previous connection: " + e.getMessage());
				}
			}

			Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:" + file);

			Bukkit.getConsoleSender().sendMessage(ChatColor.GREEN + core.getDescription().getName() + " Connected to SQLite database");
		} catch (SQLException ex) {
			core.getLogger().log(Level.SEVERE, "SQLite exception on initialize: " + ex.getMessage(), ex);
		} catch (ClassNotFoundException ex) {
			core.getLogger().log(Level.SEVERE, "You need the SQLite JDBC library. Google it. Put it in /lib folder.");
		}
	}

	public void mysqlSetup() {
		try {
			synchronized (this) {
				// Close any existing connection that might be invalid
				if (connection != null && !connection.isClosed()) {
					try {
						connection.close();
					} catch (SQLException e) {
						// Just log and continue
						core.getLogger().warning("Error closing previous connection: " + e.getMessage());
					}
				}

				String driverClass = "com.mysql.cj.jdbc.Driver";
				try {
					Class.forName(driverClass);
				} catch (ClassNotFoundException ex) {
					driverClass = "com.mysql.jdbc.Driver";
					try {
						Class.forName(driverClass);
					} catch (ClassNotFoundException e) {
						core.getLogger().severe("MySQL JDBC driver not found. Please add it to your classpath.");
						throw e;
					}
				}

				String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database + this.parameters;
				core.getLogger().info("Connecting to MySQL: " + url);

				connection = DriverManager.getConnection(url, this.username, this.password);
				Bukkit.getConsoleSender().sendMessage(ChatColor.GREEN + core.getDescription().getName() + " Connected to MySQL database");
			}
		} catch (SQLException e) {
			core.getLogger().severe("MySQL connection error: " + e.getMessage());
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			core.getLogger().severe("MySQL driver not found: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public boolean testConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				return true;
			}
		} catch (SQLException e) {
			core.getLogger().severe("Connection test failed: " + e.getMessage());
		}
		return false;
	}

	public Connection getConnection() {
		try {
			// Auto-reconnect if connection is closed or invalid
			if (connection == null || connection.isClosed()) {
				core.getLogger().info("Database connection lost, attempting to reconnect...");
				if (dboption.equalsIgnoreCase("local")) {
					sqlLiteSetup();
				} else if (dboption.equalsIgnoreCase("mysql")) {
					mysqlSetup();
				}
			}
		} catch (SQLException e) {
			core.getLogger().severe("Error checking connection: " + e.getMessage());
			// Try to reconnect
			if (dboption.equalsIgnoreCase("local")) {
				sqlLiteSetup();
			} else if (dboption.equalsIgnoreCase("mysql")) {
				mysqlSetup();
			}
		}
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private void createTable() {
		try {
			// Create main table (original one)
			String createMainTable = "CREATE TABLE IF NOT EXISTS " + table + "(" +
					"  `UUID` varchar(40) NOT NULL," +
					"  `NAME` varchar(40) NOT NULL," +
					"  `REFERRED` varchar(40) DEFAULT NULL," +
					"  `LASTREWARD` int(255) NOT NULL DEFAULT 0," +
					"  `USERIP`	varchar(255) DEFAULT NULL)";

			// Create pending referrals table (new)
			String createPendingTable = "CREATE TABLE IF NOT EXISTS " + table + "_pending(" +
					"  `ID` INTEGER PRIMARY KEY " + (dboption.equalsIgnoreCase("mysql") ? "AUTO_INCREMENT" : "AUTOINCREMENT") + "," +
					"  `PLAYER_UUID` varchar(40) NOT NULL," +
					"  `PLAYER_NAME` varchar(40) NOT NULL," +
					"  `REFERRED_BY` varchar(40) NOT NULL," +
					"  `REFERRED_BY_NAME` varchar(40) NOT NULL," +
					"  `TIMESTAMP` DATETIME DEFAULT CURRENT_TIMESTAMP)";

			Statement s = getConnection().createStatement();
			s.executeUpdate(createMainTable);
			s.executeUpdate(createPendingTable);
			s.close();

			core.getLogger().info("Tables created/verified successfully");
		} catch (SQLException e) {
			core.getLogger().severe("Error creating tables: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public boolean PlayerExists(String uuid) {
		try {
			PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM " + table + " WHERE UUID=?");
			statement.setString(1, uuid);
			ResultSet result = statement.executeQuery();
			boolean exists = result.next();
			result.close();
			statement.close();
			return exists;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerExists: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public void CreatePlayer(String playerUUID, String playerName) {
		try {
			if (PlayerExists(playerUUID)) {
				// Update name if it exists
				PreparedStatement update = getConnection().prepareStatement("UPDATE " + table + " SET NAME=? WHERE UUID=?");
				update.setString(1, playerName);
				update.setString(2, playerUUID);
				update.executeUpdate();
				update.close();
				return;
			}

			PreparedStatement insert = getConnection().prepareStatement("INSERT INTO " + table + " (UUID, NAME, REFERRED) VALUES (?,?,?)");
			insert.setString(1, playerUUID);
			insert.setString(2, playerName);
			insert.setString(3, null);
			insert.executeUpdate();
			insert.close();

			Utils.Console("[Referral] Created new player record for " + playerName + " (" + playerUUID + ")");
		} catch (SQLException e) {
			core.getLogger().severe("Error in CreatePlayer: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void ReferralPlayer(Player ref, Player refed) {
		try {
			// check if the both players are added to the database
			CreatePlayer(ref.getUniqueId().toString(), ref.getName());
			CreatePlayer(refed.getUniqueId().toString(), refed.getName());

			// CRITICAL FIX: Corrected parameter order to match old implementation
			PreparedStatement update = getConnection()
					.prepareStatement("UPDATE " + table + " SET REFERRED=?, USERIP=? WHERE UUID=?");

			update.setString(1, ref.getUniqueId().toString());  // Set referred to referrer's UUID
			update.setString(2, refed.getAddress().getHostName());  // Set IP to referred player's IP
			update.setString(3, refed.getUniqueId().toString());  // Update the referred player's record

			int rowsUpdated = update.executeUpdate();
			update.close();

			Utils.Console("[Referral] ReferralPlayer executed: " + ref.getName() + " referred " + refed.getName() + " - Rows updated: " + rowsUpdated);
		} catch (SQLException e) {
			core.getLogger().severe("Error in ReferralPlayer: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// Improved method to store offline referrals
	public void StoreOfflineReferral(String referrerUUID, String referrerName, String targetUUID, String targetName) {
		try {
			// Create player records if they don't exist - ENSURE both players exist in DB
			CreatePlayer(referrerUUID, referrerName);
			CreatePlayer(targetUUID, targetName);

			// Log the attempt for debugging
			Utils.Console("[Referral] Attempting offline referral: " + referrerName + " (" + referrerUUID + ") referring " + targetName + " (" + targetUUID + ")");

			// First, check if the target player already has a referrer
			if (PlayerReferrald(targetUUID, targetName)) {
				Utils.Console("[Referral] Target player " + targetName + " already has a referrer. Skipping update.");
				return;
			}

			// Try direct update first
			PreparedStatement directUpdate = getConnection()
					.prepareStatement("UPDATE " + table + " SET REFERRED=? WHERE UUID=?");
			directUpdate.setString(1, referrerUUID);  // Set referred to referrer's UUID
			directUpdate.setString(2, targetUUID);    // Update the target player's record
			int directRows = directUpdate.executeUpdate();
			directUpdate.close();

			Utils.Console("[Referral] StoreOfflineReferral direct update: " + referrerName + " referred " + targetName + " - Rows updated: " + directRows);

			// If direct update didn't work or affected 0 rows, try to insert into pending table
			if (directRows <= 0) {
				// First check if there's already a pending referral for this player
				PreparedStatement checkPending = getConnection().prepareStatement(
						"SELECT COUNT(*) as count FROM " + table + "_pending WHERE PLAYER_UUID=?");
				checkPending.setString(1, targetUUID);
				ResultSet pendingResult = checkPending.executeQuery();
				int pendingCount = 0;
				if (pendingResult.next()) {
					pendingCount = pendingResult.getInt("count");
				}
				pendingResult.close();
				checkPending.close();

				// Only insert if there's no pending referral
				if (pendingCount == 0) {
					PreparedStatement statement = getConnection().prepareStatement(
							"INSERT INTO " + table + "_pending (PLAYER_UUID, PLAYER_NAME, REFERRED_BY, REFERRED_BY_NAME) VALUES (?, ?, ?, ?)");
					statement.setString(1, targetUUID);
					statement.setString(2, targetName);
					statement.setString(3, referrerUUID);
					statement.setString(4, referrerName);
					int pendingRows = statement.executeUpdate();
					statement.close();
					Utils.Console("[Referral] Added to pending referrals: " + referrerName + " referred " + targetName + " - Rows inserted: " + pendingRows);
				} else {
					Utils.Console("[Referral] Player " + targetName + " already has a pending referral. Skipping insert.");
				}
			}

			// Verify the referral was stored properly
			PreparedStatement verify = getConnection().prepareStatement(
					"SELECT REFERRED FROM " + table + " WHERE UUID=?");
			verify.setString(1, targetUUID);
			ResultSet verifyResult = verify.executeQuery();
			if (verifyResult.next()) {
				String storedReferrer = verifyResult.getString("REFERRED");
				if (storedReferrer != null && storedReferrer.equals(referrerUUID)) {
					Utils.Console("[Referral] Verified referral stored correctly for " + targetName);
				} else {
					Utils.Console("[Referral] WARNING: Referral verification failed for " + targetName +
							". Expected: " + referrerUUID + ", Found: " + storedReferrer);
				}
			}
			verifyResult.close();
			verify.close();

		} catch (SQLException e) {
			core.getLogger().severe("Error in StoreOfflineReferral: " + e.getMessage());
			e.printStackTrace();
		}
	}


	// New method to check for pending referrals when a player logs in
	public void CheckPendingReferrals(Player player) {
		try {
			String playerUUID = player.getUniqueId().toString();

			// Check if player has any pending referrals
			PreparedStatement statement = getConnection().prepareStatement(
					"SELECT * FROM " + table + "_pending WHERE PLAYER_UUID = ?");
			statement.setString(1, playerUUID);
			ResultSet resultSet = statement.executeQuery();

			if (resultSet.next()) {
				String referredByUUID = resultSet.getString("REFERRED_BY");
				String referredByName = resultSet.getString("REFERRED_BY_NAME");
				int id = resultSet.getInt("ID");

				// Create players if they don't exist
				CreatePlayer(referredByUUID, referredByName);
				CreatePlayer(playerUUID, player.getName());

				// Process the referral
				String ip = player.getAddress().getHostName();

				// Update main table
				PreparedStatement update = getConnection()
						.prepareStatement("UPDATE " + table + " SET REFERRED=?, USERIP=? WHERE UUID=?");
				update.setString(1, referredByUUID);
				update.setString(2, ip);
				update.setString(3, playerUUID);
				update.executeUpdate();
				update.close();

				// Delete from pending_referrals
				PreparedStatement deleteStatement = getConnection().prepareStatement("DELETE FROM " + table + "_pending WHERE ID = ?");
				deleteStatement.setInt(1, id);
				deleteStatement.executeUpdate();
				deleteStatement.close();

				// Give rewards to the player
				core.UseCommands(core.config.playerRefers, player);

				// Notify the player - Using the config message
				String message = core.config.referralProcessed.replace("%player_name%", referredByName);
				Utils.SendMessage(player, message);

				// Log the action
				Utils.Console("[Referral] Processed pending referral: " + referredByName + " referred " + player.getName());
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			core.getLogger().severe("Error in CheckPendingReferrals: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public boolean PlayerReferrald(String playerUUID, String playerName) {
		try {
			if (!PlayerExists(playerUUID)) {
				CreatePlayer(playerUUID, playerName);
				return false;
			}

			PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM " + table + " WHERE UUID=?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			if (result.next() && result.getString("REFERRED") == null) {
				result.close();
				statement.close();
				return false;
			}

			result.close();
			statement.close();
			return true;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerReferrald: " + e.getMessage());
			e.printStackTrace();
			return true; // Default to true to prevent duplicate referrals in case of error
		}
	}

	public String PlayerReferraldBy(String playerUUID) {
		try {
			if (!PlayerExists(playerUUID)) {
				return null;
			}

			PreparedStatement statement = getConnection().prepareStatement("SELECT REFERRED FROM " + table + " WHERE UUID=?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			String referred = null;
			if (result.next() && result.getString("REFERRED") != null) {
				referred = result.getString("REFERRED");
			}

			result.close();
			statement.close();
			return referred;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerReferraldBy: " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}

	public String PlayerReferraldByName(String playerUUID) {
		try {
			if (!PlayerExists(playerUUID)) {
				return "None";
			}

			PreparedStatement statement = getConnection().prepareStatement(
					"SELECT COALESCE(t2.NAME, 'None') AS ReferredBy FROM " + table + " t1 LEFT JOIN " + table + " t2 ON t1.REFERRED = t2.UUID WHERE t1.UUID = ?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			String referredBy = "None";
			if (result.next() && result.getString("ReferredBy") != null) {
				referredBy = result.getString("ReferredBy");
			}

			result.close();
			statement.close();
			return referredBy;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerReferraldByName: " + e.getMessage());
			e.printStackTrace();
			return "None";
		}
	}

	public boolean PlayerReset(String player) {
		try {
			if (!PlayerExists(player)) {
				return true;
			}

			PreparedStatement statement = getConnection().prepareStatement(
					"UPDATE " + table + " SET REFERRED=null, LASTREWARD=0, USERIP=null WHERE UUID=?");
			statement.setString(1, player);
			statement.executeUpdate();
			statement.close();
			return true;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerReset: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public boolean PlayerRemove(String player) {
		try {
			if (!PlayerExists(player)) {
				return true;
			}

			// Delete from main table
			PreparedStatement statement = getConnection().prepareStatement("DELETE FROM " + table + " WHERE UUID=?");
			statement.setString(1, player);
			statement.executeUpdate();
			statement.close();

			// Delete from pending table
			statement = getConnection().prepareStatement("DELETE FROM " + table + "_pending WHERE PLAYER_UUID=? OR REFERRED_BY=?");
			statement.setString(1, player);
			statement.setString(2, player);
			statement.executeUpdate();
			statement.close();
			return true;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerRemove: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public int GetReferrals(String playerUUID, String playerName) {
		try {
			if (!PlayerExists(playerUUID)) {
				return 0;
			}

			PreparedStatement statement = getConnection().prepareStatement("SELECT COUNT(*) as total FROM " + table + " WHERE REFERRED=?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			int total = 0;
			if (result.next()) {
				total = result.getInt("total");
			}

			result.close();
			statement.close();
			return total;
		} catch (SQLException e) {
			core.getLogger().severe("Error in GetReferrals: " + e.getMessage());
			e.printStackTrace();
			return 0;
		}
	}

	public List<TopPlayer> GetTopPlayers(int min, int max) {
		List<TopPlayer> topPlayer = new ArrayList<TopPlayer>();
		try {
			PreparedStatement statement;

			if (dboption.equalsIgnoreCase("mysql")) {
				statement = getConnection().prepareStatement(
						"SELECT U.UUID, U.NAME, (SELECT COUNT(*) FROM " + table + " US WHERE US.REFERRED=U.UUID) as REFTOTAL " +
								"FROM " + table + " U ORDER BY REFTOTAL DESC, NAME ASC LIMIT ?, ?");
			} else {
				statement = getConnection().prepareStatement(
						"SELECT U.UUID, U.NAME, (SELECT COUNT(*) FROM " + table + " US WHERE US.REFERRED=U.UUID) as REFTOTAL " +
								"FROM " + table + " U ORDER BY REFTOTAL DESC, NAME ASC LIMIT ?, ?");
			}

			statement.setInt(1, min);
			statement.setInt(2, max);
			ResultSet result = statement.executeQuery();

			int position = min + 1;
			while (result.next()) {
				// create and add top player
				topPlayer.add(new TopPlayer(result.getString("UUID"), result.getString("NAME"), position, result.getInt("REFTOTAL")));
				position++;
			}

			result.close();
			statement.close();
		} catch (SQLException e) {
			core.getLogger().severe("Error in GetTopPlayers: " + e.getMessage());
			e.printStackTrace();
		}
		return topPlayer;
	}

	public int GetLastReward(String playerUUID, String playerName) {
		try {
			if (!PlayerExists(playerUUID)) {
				CreatePlayer(playerUUID, playerName);
				return 0;
			}

			PreparedStatement statement = getConnection().prepareStatement("SELECT LASTREWARD as amount FROM " + table + " WHERE UUID=?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			int amount = 0;
			if (result.next()) {
				amount = result.getInt("amount");
			}

			result.close();
			statement.close();
			return amount;
		} catch (SQLException e) {
			core.getLogger().severe("Error in GetLastReward: " + e.getMessage());
			e.printStackTrace();
			return 0;
		}
	}

	public int GetUsedRefIP(String playerUUID, String ip) {
		try {
			if (!PlayerExists(playerUUID)) {
				return 0;
			}

			PreparedStatement statement = getConnection().prepareStatement("SELECT COUNT(*) as total FROM " + table + " WHERE USERIP=? AND NOT UUID=?");
			statement.setString(1, ip);
			statement.setString(2, playerUUID);
			ResultSet result = statement.executeQuery();

			int total = 0;
			if (result.next()) {
				total = result.getInt("total");
			}

			result.close();
			statement.close();
			return total;
		} catch (SQLException e) {
			core.getLogger().severe("Error in GetUsedRefIP: " + e.getMessage());
			e.printStackTrace();
			return 0;
		}
	}

	public boolean ResetAll() {
		try {
			PreparedStatement statement = getConnection().prepareStatement("UPDATE " + table + " SET REFERRED=null, LASTREWARD=0, USERIP=null");
			statement.executeUpdate();
			statement.close();
			return true;
		} catch (SQLException e) {
			core.getLogger().severe("Error in ResetAll: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public boolean RemoveAll() {
		try {
			PreparedStatement statement = getConnection().prepareStatement("DELETE FROM " + table);
			statement.executeUpdate();
			statement.close();

			statement = getConnection().prepareStatement("DELETE FROM " + table + "_pending");
			statement.executeUpdate();
			statement.close();
			return true;
		} catch (SQLException e) {
			core.getLogger().severe("Error in RemoveAll: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public void LastRewardUpdate(Player player, int lastReward) {
		try {
			// check if the player is added to the database
			CreatePlayer(player.getUniqueId().toString(), player.getName());

			PreparedStatement update = getConnection().prepareStatement("UPDATE " + table + " SET LASTREWARD=? WHERE UUID=?");
			update.setInt(1, lastReward);
			update.setString(2, player.getUniqueId().toString());
			update.executeUpdate();
			update.close();
		} catch (SQLException e) {
			core.getLogger().severe("Error in LastRewardUpdate: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public int GetPlayerPosition(String name) {
		try {
			PreparedStatement statement = getConnection().prepareStatement(
					"SELECT (SELECT COUNT(*) FROM " + table + " US WHERE US.NAME <= U.NAME) AS position FROM " + table + " U WHERE U.NAME=?");
			statement.setString(1, name);
			ResultSet result = statement.executeQuery();

			int position = 999999;
			if (result.next()) {
				position = result.getInt("position");
			}

			result.close();
			statement.close();
			return position;
		} catch (SQLException e) {
			core.getLogger().severe("Error in GetPlayerPosition: " + e.getMessage());
			e.printStackTrace();
			return 999999;
		}
	}

	public void CloseConnection() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
				core.getLogger().info("Database connection closed successfully");
			}
		} catch (SQLException e) {
			core.getLogger().severe("Error closing database connection: " + e.getMessage());
			e.printStackTrace();
		}
	}
}