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
				// Execute a simple query to really test the connection
				try (Statement stmt = connection.createStatement()) {
					stmt.executeQuery("SELECT 1");
					return true;
				}
			}
		} catch (SQLException e) {
			core.getLogger().severe("Connection test failed: " + e.getMessage());
			// Try to reconnect
			if (dboption.equalsIgnoreCase("local")) {
				sqlLiteSetup();
			} else if (dboption.equalsIgnoreCase("mysql")) {
				mysqlSetup();
			}
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
			} else {
				// Test if connection is actually valid
				try {
					if (!testConnection()) {
						core.getLogger().info("Database connection invalid, attempting to reconnect...");
						if (dboption.equalsIgnoreCase("local")) {
							sqlLiteSetup();
						} else if (dboption.equalsIgnoreCase("mysql")) {
							mysqlSetup();
						}
					}
				} catch (Exception e) {
					core.getLogger().severe("Error testing connection: " + e.getMessage());
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
					"  `USERIP`	varchar(255) DEFAULT NULL," +
					"  `LAST_UPDATED` DATETIME DEFAULT CURRENT_TIMESTAMP)";

			// Create pending referrals table (new)
			String createPendingTable = "CREATE TABLE IF NOT EXISTS " + table + "_pending(" +
					"  `ID` INTEGER PRIMARY KEY " + (dboption.equalsIgnoreCase("mysql") ? "AUTO_INCREMENT" : "AUTOINCREMENT") + "," +
					"  `PLAYER_UUID` varchar(40) NOT NULL," +
					"  `PLAYER_NAME` varchar(40) NOT NULL," +
					"  `REFERRED_BY` varchar(40) NOT NULL," +
					"  `REFERRED_BY_NAME` varchar(40) NOT NULL," +
					"  `TIMESTAMP` DATETIME DEFAULT CURRENT_TIMESTAMP)";

			// Create index for faster lookups
			String createIndexUUID = "CREATE INDEX IF NOT EXISTS idx_" + table + "_uuid ON " + table + " (UUID)";
			String createIndexReferred = "CREATE INDEX IF NOT EXISTS idx_" + table + "_referred ON " + table + " (REFERRED)";
			String createIndexPendingPlayerUUID = "CREATE INDEX IF NOT EXISTS idx_" + table + "_pending_player_uuid ON " + table + "_pending (PLAYER_UUID)";
			String createIndexPendingReferredBy = "CREATE INDEX IF NOT EXISTS idx_" + table + "_pending_referred_by ON " + table + "_pending (REFERRED_BY)";

			Statement s = getConnection().createStatement();
			s.executeUpdate(createMainTable);
			s.executeUpdate(createPendingTable);

			// Add indexes for better performance
			try {
				s.executeUpdate(createIndexUUID);
				s.executeUpdate(createIndexReferred);
				s.executeUpdate(createIndexPendingPlayerUUID);
				s.executeUpdate(createIndexPendingReferredBy);
				core.getLogger().info("Database indexes created/verified successfully");
			} catch (SQLException e) {
				// Some databases might not support this syntax, so we'll just log it
				core.getLogger().warning("Could not create indexes: " + e.getMessage());
			}

			s.close();

			core.getLogger().info("Tables created/verified successfully");
		} catch (SQLException e) {
			core.getLogger().severe("Error creating tables: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public boolean PlayerExists(String uuid) {
		if (uuid == null || uuid.isEmpty()) {
			core.getLogger().warning("Attempted to check if player exists with null or empty UUID");
			return false;
		}

		try {
			PreparedStatement statement = getConnection().prepareStatement("SELECT 1 FROM " + table + " WHERE UUID=?");
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
		if (playerUUID == null || playerUUID.isEmpty() || playerName == null || playerName.isEmpty()) {
			core.getLogger().warning("Attempted to create player with null or empty UUID/name: " + playerUUID + "/" + playerName);
			return;
		}

		try {
			if (PlayerExists(playerUUID)) {
				// Update name if it exists
				PreparedStatement update = getConnection().prepareStatement("UPDATE " + table + " SET NAME=?, LAST_UPDATED=CURRENT_TIMESTAMP WHERE UUID=?");
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

			Utils.Console("Created new player record for " + playerName + " (" + playerUUID + ")");
		} catch (SQLException e) {
			core.getLogger().severe("Error in CreatePlayer: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void ReferralPlayer(Player referrer, Player referred) {
		if (referrer == null || referred == null) {
			core.getLogger().warning("Attempted to process referral with null player(s)");
			return;
		}

		try {
			// check if the both players are added to the database
			CreatePlayer(referrer.getUniqueId().toString(), referrer.getName());
			CreatePlayer(referred.getUniqueId().toString(), referred.getName());

			// Store the referred player's UUID in the referrer's record
			PreparedStatement update = getConnection()
					.prepareStatement("UPDATE " + table + " SET REFERRED=?, USERIP=?, LAST_UPDATED=CURRENT_TIMESTAMP WHERE UUID=?");

			// When player A refers player B, player B's UUID should be stored in player A's record
			update.setString(1, referred.getUniqueId().toString());  // Store referred player's UUID in REFERRED column
			update.setString(2, referrer.getAddress().getHostName());  // Set IP to referrer's IP
			update.setString(3, referrer.getUniqueId().toString());  // Update the referrer's record

			int rowsUpdated = update.executeUpdate();
			update.close();

			Utils.Console("ReferralPlayer executed: " + referrer.getName() + " referred " + referred.getName() + " - Rows updated: " + rowsUpdated);

			// If no rows were updated, log a warning
			if (rowsUpdated <= 0) {
				core.getLogger().warning("No rows were updated when processing referral: " + referrer.getName() + " -> " + referred.getName());
			}
		} catch (SQLException e) {
			core.getLogger().severe("Error in ReferralPlayer: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// Improved method to store offline referrals
	public void StoreOfflineReferral(String referrerUUID, String referrerName, String targetUUID, String targetName) {
		if (referrerUUID == null || referrerUUID.isEmpty() || targetUUID == null || targetUUID.isEmpty()) {
			core.getLogger().warning("Attempted to store offline referral with null or empty UUID(s): " +
					referrerUUID + "/" + targetUUID);
			return;
		}

		try {
			// Create player records if they don't exist - ENSURE both players exist in DB
			CreatePlayer(referrerUUID, referrerName);
			CreatePlayer(targetUUID, targetName);

			// Log the attempt for debugging
			Utils.Console("Attempting offline referral: " + referrerName + " (" + referrerUUID + ") referring " + targetName + " (" + targetUUID + ")");

			// Update the referrer's record directly - store the target's UUID in the referrer's REFERRED column
			PreparedStatement update = getConnection()
					.prepareStatement("UPDATE " + table + " SET REFERRED=?, LAST_UPDATED=CURRENT_TIMESTAMP WHERE UUID=?");
			update.setString(1, targetUUID);  // Store target's UUID in referrer's REFERRED column
			update.setString(2, referrerUUID);  // Update the referrer's record
			int rowsUpdated = update.executeUpdate();
			update.close();

			// Also add to pending table for the target player to receive rewards on login
			PreparedStatement insertPending = getConnection()
					.prepareStatement("INSERT INTO " + table + "_pending (PLAYER_UUID, PLAYER_NAME, REFERRED_BY, REFERRED_BY_NAME) VALUES (?, ?, ?, ?)");
			insertPending.setString(1, targetUUID);  // The player who will receive rewards on login
			insertPending.setString(2, targetName);
			insertPending.setString(3, referrerUUID);  // The player who referred them
			insertPending.setString(4, referrerName);
			insertPending.executeUpdate();
			insertPending.close();

			Utils.Console("StoreOfflineReferral executed: " + referrerName + " referred " + targetName + " - Rows updated: " + rowsUpdated);

			// If no rows were updated, log a warning
			if (rowsUpdated <= 0) {
				core.getLogger().warning("No rows were updated when processing offline referral: " + referrerName + " -> " + targetName);
			}

		} catch (SQLException e) {
			core.getLogger().severe("Error in StoreOfflineReferral: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// Method to check for pending referrals when a player logs in
	public void CheckPendingReferrals(Player player) {
		if (player == null) {
			core.getLogger().warning("Attempted to check pending referrals for null player");
			return;
		}

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

				// Check if the referrer already has a referral in the main table
				PreparedStatement checkExisting = getConnection().prepareStatement(
						"SELECT REFERRED FROM " + table + " WHERE UUID=?");
				checkExisting.setString(1, referredByUUID);
				ResultSet existingResult = checkExisting.executeQuery();

				boolean alreadyReferred = false;
				if (existingResult.next()) {
					String existingReferral = existingResult.getString("REFERRED");
					if (existingReferral != null && !existingReferral.isEmpty()) {
						// Check if the existing referral is for this player
						if (!existingReferral.equals(playerUUID)) {
							alreadyReferred = true;
							Utils.Console("Referrer " + referredByName + " already has a different referral in main table: " + existingReferral);
						}
					}
				}
				existingResult.close();
				checkExisting.close();

				// Only update if not already referred someone else
				if (!alreadyReferred) {
					// Process the referral
					String ip = player.getAddress().getHostName();

					// Update main table - Referrer's record gets the player's UUID
					PreparedStatement update = getConnection()
							.prepareStatement("UPDATE " + table + " SET REFERRED=?, USERIP=?, LAST_UPDATED=CURRENT_TIMESTAMP WHERE UUID=?");
					update.setString(1, playerUUID);  // Set referred to the UUID of the player who was referred
					update.setString(2, ip);
					update.setString(3, referredByUUID);  // Update the referrer's record
					int rowsUpdated = update.executeUpdate();
					update.close();

					Utils.Console("Updated main table for referrer " + referredByName + " - Rows updated: " + rowsUpdated);

					// Give rewards to the player
					core.UseCommands(core.config.playerRefers, player);

					// Notify the player - Using the config message with placeholder
					String message = core.config.referralProcessed.replace("%player_name%", referredByName);
					Utils.SendMessage(player, message);

					// Log the action
					Utils.Console("Processed pending referral: " + referredByName + " referred " + player.getName());
				}

				// Delete from pending_referrals regardless of whether we processed it or not
				PreparedStatement deleteStatement = getConnection().prepareStatement("DELETE FROM " + table + "_pending WHERE ID = ?");
				deleteStatement.setInt(1, id);
				deleteStatement.executeUpdate();
				deleteStatement.close();
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			core.getLogger().severe("Error in CheckPendingReferrals: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public boolean PlayerReferrald(String playerUUID, String playerName) {
		if (playerUUID == null || playerUUID.isEmpty()) {
			core.getLogger().warning("Attempted to check if player is referred with null or empty UUID: " + playerUUID);
			return false;
		}

		try {
			if (!PlayerExists(playerUUID)) {
				CreatePlayer(playerUUID, playerName);
				return false;
			}

			PreparedStatement statement = getConnection().prepareStatement("SELECT REFERRED FROM " + table + " WHERE UUID=?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			boolean hasReferrer = false;
			if (result.next()) {
				String referred = result.getString("REFERRED");
				hasReferrer = (referred != null && !referred.isEmpty());
			}

			result.close();
			statement.close();
			return hasReferrer;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerReferrald: " + e.getMessage());
			e.printStackTrace();
			return true; // Default to true to prevent duplicate referrals in case of error
		}
	}

	public String PlayerReferraldBy(String playerUUID) {
		if (playerUUID == null || playerUUID.isEmpty()) {
			core.getLogger().warning("Attempted to get referrer with null or empty UUID: " + playerUUID);
			return null;
		}

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
		if (playerUUID == null || playerUUID.isEmpty()) {
			core.getLogger().warning("Attempted to get referrer name with null or empty UUID: " + playerUUID);
			return "None";
		}

		try {
			if (!PlayerExists(playerUUID)) {
				return "None";
			}

			// Get the name of the player whose UUID is in the REFERRED column
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
		if (player == null || player.isEmpty()) {
			core.getLogger().warning("Attempted to reset player with null or empty UUID: " + player);
			return false;
		}

		try {
			if (!PlayerExists(player)) {
				return true;
			}

			PreparedStatement statement = getConnection().prepareStatement(
					"UPDATE " + table + " SET REFERRED=null, LASTREWARD=0, USERIP=null, LAST_UPDATED=CURRENT_TIMESTAMP WHERE UUID=?");
			statement.setString(1, player);
			int rowsUpdated = statement.executeUpdate();
			statement.close();

			Utils.Console("Reset player " + player + " - Rows updated: " + rowsUpdated);
			return rowsUpdated > 0;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerReset: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public boolean PlayerRemove(String player) {
		if (player == null || player.isEmpty()) {
			core.getLogger().warning("Attempted to remove player with null or empty UUID: " + player);
			return false;
		}

		try {
			if (!PlayerExists(player)) {
				return true;
			}

			// Delete from main table
			PreparedStatement statement = getConnection().prepareStatement("DELETE FROM " + table + " WHERE UUID=?");
			statement.setString(1, player);
			int mainRowsDeleted = statement.executeUpdate();
			statement.close();

			// Delete from pending table
			statement = getConnection().prepareStatement("DELETE FROM " + table + "_pending WHERE PLAYER_UUID=? OR REFERRED_BY=?");
			statement.setString(1, player);
			statement.setString(2, player);
			int pendingRowsDeleted = statement.executeUpdate();
			statement.close();

			Utils.Console("Removed player " + player + " - Main rows deleted: " + mainRowsDeleted +
					", Pending rows deleted: " + pendingRowsDeleted);
			return mainRowsDeleted > 0;
		} catch (SQLException e) {
			core.getLogger().severe("Error in PlayerRemove: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public int GetReferrals(String playerUUID, String playerName) {
		if (playerUUID == null || playerUUID.isEmpty()) {
			core.getLogger().warning("Attempted to get referrals with null or empty UUID: " + playerUUID);
			return 0;
		}

		try {
			if (!PlayerExists(playerUUID)) {
				return 0;
			}

			// Count how many players have this player's UUID in their REFERRED column
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
		if (playerUUID == null || playerUUID.isEmpty()) {
			core.getLogger().warning("Attempted to get last reward with null or empty UUID: " + playerUUID);
			return 0;
		}

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
		if (playerUUID == null || playerUUID.isEmpty() || ip == null || ip.isEmpty()) {
			core.getLogger().warning("Attempted to get used ref IP with null or empty parameters: " + playerUUID + "/" + ip);
			return 0;
		}

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
			PreparedStatement statement = getConnection().prepareStatement(
					"UPDATE " + table + " SET REFERRED=null, LASTREWARD=0, USERIP=null, LAST_UPDATED=CURRENT_TIMESTAMP");
			int rowsUpdated = statement.executeUpdate();
			statement.close();

			Utils.Console("Reset all players - Rows updated: " + rowsUpdated);
			return rowsUpdated > 0;
		} catch (SQLException e) {
			core.getLogger().severe("Error in ResetAll: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public boolean RemoveAll() {
		try {
			PreparedStatement statement = getConnection().prepareStatement("DELETE FROM " + table);
			int mainRowsDeleted = statement.executeUpdate();
			statement.close();

			statement = getConnection().prepareStatement("DELETE FROM " + table + "_pending");
			int pendingRowsDeleted = statement.executeUpdate();
			statement.close();

			Utils.Console("Removed all players - Main rows deleted: " + mainRowsDeleted +
					", Pending rows deleted: " + pendingRowsDeleted);
			return true;
		} catch (SQLException e) {
			core.getLogger().severe("Error in RemoveAll: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public void LastRewardUpdate(Player player, int lastReward) {
		if (player == null) {
			core.getLogger().warning("Attempted to update last reward for null player");
			return;
		}

		try {
			// check if the player is added to the database
			CreatePlayer(player.getUniqueId().toString(), player.getName());

			PreparedStatement update = getConnection().prepareStatement(
					"UPDATE " + table + " SET LASTREWARD=?, LAST_UPDATED=CURRENT_TIMESTAMP WHERE UUID=?");
			update.setInt(1, lastReward);
			update.setString(2, player.getUniqueId().toString());
			int rowsUpdated = update.executeUpdate();
			update.close();

			Utils.Console("Updated last reward for player " + player.getName() + " to " + lastReward +
					" - Rows updated: " + rowsUpdated);
		} catch (SQLException e) {
			core.getLogger().severe("Error in LastRewardUpdate: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public int GetPlayerPosition(String name) {
		if (name == null || name.isEmpty()) {
			core.getLogger().warning("Attempted to get player position with null or empty name: " + name);
			return 999999;
		}

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

	/**
	 * Get the number of times a player has been referred by others
	 * This is the opposite of GetReferrals - it counts how many times this player appears in the REFERRED column
	 * @param playerUUID The UUID of the player
	 * @param playerName The name of the player (for creating if neede d)
	 * @return The number of times the player has been referred
	 */
	public int GetTimesReferred(String playerUUID, String playerName) {
		if (playerUUID == null || playerUUID.isEmpty()) {
			core.getLogger().warning("Attempted to get times referred with null or empty UUID: " + playerUUID);
			return 0;
		}

		try {
			if (!PlayerExists(playerUUID)) {
				CreatePlayer(playerUUID, playerName);
				return 0;
			}

			// Check if this player has been referred by anyone
			PreparedStatement statement = getConnection().prepareStatement(
					"SELECT CASE WHEN REFERRED IS NULL THEN 0 ELSE 1 END as referred FROM " + table + " WHERE UUID=?");
			statement.setString(1, playerUUID);
			ResultSet result = statement.executeQuery();

			int timesReferred = 0;
			if (result.next()) {
				timesReferred = result.getInt("referred");
			}

			result.close();
			statement.close();
			return timesReferred;
		} catch (SQLException e) {
			core.getLogger().severe("Error in GetTimesReferred: " + e.getMessage());
			e.printStackTrace();
			return 0;
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