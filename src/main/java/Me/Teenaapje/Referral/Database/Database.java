package Me.Teenaapje.Referral.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bukkit.entity.Player;

import Me.Teenaapje.Referral.ReferralCore;
import Me.Teenaapje.Referral.Utils.TopPlayer;
import Me.Teenaapje.Referral.Utils.Utils;

public class Database {
	// the core
	ReferralCore core = ReferralCore.core;

	// connection
	Connection connection;

	// init Database
	public Database() {
		try {
			// create connection
			connection = DriverManager.getConnection("jdbc:sqlite:" + core.getDataFolder() + "/database.db");

			// create tables
			Statement statement = connection.createStatement();
			statement.execute("CREATE TABLE IF NOT EXISTS referrals (id INTEGER PRIMARY KEY AUTOINCREMENT, player_uuid TEXT, player_name TEXT, referred_by TEXT, referred_by_name TEXT, ip TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)");
			statement.execute("CREATE TABLE IF NOT EXISTS rewards (id INTEGER PRIMARY KEY AUTOINCREMENT, player_uuid TEXT, player_name TEXT, last_reward INTEGER DEFAULT 0)");
			statement.execute("CREATE TABLE IF NOT EXISTS pending_referrals (id INTEGER PRIMARY KEY AUTOINCREMENT, player_uuid TEXT, player_name TEXT, referred_by TEXT, referred_by_name TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// close connection
	public void CloseConnection() {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// referral player
	public void ReferralPlayer(Player player, Player target) {
		try {
			// get the player info
			String playerUUID = target.getUniqueId().toString();
			String playerName = target.getName();
			String referredByUUID = player.getUniqueId().toString();
			String referredByName = player.getName();
			String ip = target.getAddress().getHostName();

			// insert into database
			PreparedStatement statement = connection.prepareStatement("INSERT INTO referrals (player_uuid, player_name, referred_by, referred_by_name, ip) VALUES (?, ?, ?, ?, ?)");
			statement.setString(1, playerUUID);
			statement.setString(2, playerName);
			statement.setString(3, referredByUUID);
			statement.setString(4, referredByName);
			statement.setString(5, ip);
			statement.executeUpdate();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Store offline referral
	public void StoreOfflineReferral(String referrerUUID, String referrerName, String targetUUID, String targetName) {
		try {
			// Insert into pending_referrals table
			PreparedStatement statement = connection.prepareStatement("INSERT INTO pending_referrals (player_uuid, player_name, referred_by, referred_by_name) VALUES (?, ?, ?, ?)");
			statement.setString(1, targetUUID);
			statement.setString(2, targetName);
			statement.setString(3, referrerUUID);
			statement.setString(4, referrerName);
			statement.executeUpdate();
			statement.close();

			// Log the action
			Utils.Console("[Referral] Stored offline referral: " + referrerName + " referred " + targetName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Check for pending referrals when a player logs in
	public void CheckPendingReferrals(Player player) {
		try {
			String playerUUID = player.getUniqueId().toString();

			// Check if player has any pending referrals
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM pending_referrals WHERE player_uuid = ?");
			statement.setString(1, playerUUID);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String referredByUUID = resultSet.getString("referred_by");
				String referredByName = resultSet.getString("referred_by_name");
				int id = resultSet.getInt("id");

				// Process the referral
				String ip = player.getAddress().getHostName();

				// Insert into main referrals table
				PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO referrals (player_uuid, player_name, referred_by, referred_by_name, ip) VALUES (?, ?, ?, ?, ?)");
				insertStatement.setString(1, playerUUID);
				insertStatement.setString(2, player.getName());
				insertStatement.setString(3, referredByUUID);
				insertStatement.setString(4, referredByName);
				insertStatement.setString(5, ip);
				insertStatement.executeUpdate();
				insertStatement.close();

				// Delete from pending_referrals
				PreparedStatement deleteStatement = connection.prepareStatement("DELETE FROM pending_referrals WHERE id = ?");
				deleteStatement.setInt(1, id);
				deleteStatement.executeUpdate();
				deleteStatement.close();

				// Give rewards to the player
				core.UseCommands(core.config.playerRefers, player);

				// Notify the player
				Utils.SendMessage(player, "§aYou have been referred by §e" + referredByName + "§a and received your rewards!");

				// Log the action
				Utils.Console("[Referral] Processed pending referral: " + referredByName + " referred " + player.getName());
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// check if player is referred
	public boolean PlayerReferrald(String uuid, String name) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM referrals WHERE player_uuid = ?");
			statement.setString(1, uuid);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				resultSet.close();
				statement.close();
				return true;
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return false;
	}

	// get player referred by
	public String PlayerReferraldBy(String uuid) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM referrals WHERE player_uuid = ?");
			statement.setString(1, uuid);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				String referredBy = resultSet.getString("referred_by");
				resultSet.close();
				statement.close();
				return referredBy;
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	// get player referred by name
	public String PlayerReferraldByName(String uuid) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM referrals WHERE player_uuid = ?");
			statement.setString(1, uuid);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				String referredByName = resultSet.getString("referred_by_name");
				resultSet.close();
				statement.close();
				return referredByName;
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	// get used ref ip
	public int GetUsedRefIP(String uuid, String ip) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM referrals WHERE referred_by = ? AND ip = ?");
			statement.setString(1, uuid);
			statement.setString(2, ip);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				int count = resultSet.getInt(1);
				resultSet.close();
				statement.close();
				return count;
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}

	// get last reward
	public int GetLastReward(String uuid, String name) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM rewards WHERE player_uuid = ?");
			statement.setString(1, uuid);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				int lastReward = resultSet.getInt("last_reward");
				resultSet.close();
				statement.close();
				return lastReward;
			}

			resultSet.close();
			statement.close();

			// player is not in database, add player
			statement = connection.prepareStatement("INSERT INTO rewards (player_uuid, player_name, last_reward) VALUES (?, ?, ?)");
			statement.setString(1, uuid);
			statement.setString(2, name);
			statement.setInt(3, 0);
			statement.executeUpdate();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}

	// set last reward
	public void SetLastReward(String uuid, String name, int lastReward) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM rewards WHERE player_uuid = ?");
			statement.setString(1, uuid);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				// update player
				statement = connection.prepareStatement("UPDATE rewards SET last_reward = ? WHERE player_uuid = ?");
				statement.setInt(1, lastReward);
				statement.setString(2, uuid);
				statement.executeUpdate();
				statement.close();
			} else {
				// add player
				statement = connection.prepareStatement("INSERT INTO rewards (player_uuid, player_name, last_reward) VALUES (?, ?, ?)");
				statement.setString(1, uuid);
				statement.setString(2, name);
				statement.setInt(3, lastReward);
				statement.executeUpdate();
				statement.close();
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// get referrals
	public int GetReferrals(String uuid, String name) {
		try {
			// check if player is in database
			PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM referrals WHERE referred_by = ?");
			statement.setString(1, uuid);
			ResultSet resultSet = statement.executeQuery();

			// check if player is in database
			if (resultSet.next()) {
				int count = resultSet.getInt(1);
				resultSet.close();
				statement.close();
				return count;
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}

	// Get top players
	public List<TopPlayer> GetTopPlayers(int offset, int limit) {
		List<TopPlayer> topPlayers = new ArrayList<>();
		try {
			// If limit is 0, set it to a high number to get all records
			if (limit == 0) {
				limit = 100;
			}

			PreparedStatement statement = connection.prepareStatement(
					"SELECT referred_by, referred_by_name, COUNT(*) as count " +
							"FROM referrals " +
							"GROUP BY referred_by " +
							"ORDER BY count DESC " +
							"LIMIT ? OFFSET ?"
			);
			statement.setInt(1, limit);
			statement.setInt(2, offset);
			ResultSet resultSet = statement.executeQuery();

			int position = offset + 1;
			while (resultSet.next()) {
				String uuid = resultSet.getString("referred_by");
				String name = resultSet.getString("referred_by_name");
				int count = resultSet.getInt("count");

				topPlayers.add(new TopPlayer(uuid, name, count, position));
				position++;
			}

			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return topPlayers;
	}

	// Remove all referrals
	public void RemoveAll() {
		try {
			Statement statement = connection.createStatement();
			statement.execute("DELETE FROM referrals");
			statement.execute("DELETE FROM rewards");
			statement.execute("DELETE FROM pending_referrals");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Reset all rewards
	public void ResetAll() {
		try {
			Statement statement = connection.createStatement();
			statement.execute("DELETE FROM rewards");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Remove player's referrals
	public boolean PlayerRemove(String playerName) {
		try {
			// Get player UUID
			UUID playerUUID = core.GetPlayerUUID(playerName);
			if (playerUUID == null) {
				return false;
			}

			String uuid = playerUUID.toString();

			// Delete from referrals table
			PreparedStatement statement = connection.prepareStatement("DELETE FROM referrals WHERE player_uuid = ? OR referred_by = ?");
			statement.setString(1, uuid);
			statement.setString(2, uuid);
			statement.executeUpdate();

			// Delete from rewards table
			statement = connection.prepareStatement("DELETE FROM rewards WHERE player_uuid = ?");
			statement.setString(1, uuid);
			statement.executeUpdate();

			// Delete from pending_referrals table
			statement = connection.prepareStatement("DELETE FROM pending_referrals WHERE player_uuid = ? OR referred_by = ?");
			statement.setString(1, uuid);
			statement.setString(2, uuid);
			statement.executeUpdate();

			statement.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	// Reset player's rewards
	public boolean PlayerReset(String playerName) {
		try {
			// Get player UUID
			UUID playerUUID = core.GetPlayerUUID(playerName);
			if (playerUUID == null) {
				return false;
			}

			String uuid = playerUUID.toString();

			// Delete from rewards table
			PreparedStatement statement = connection.prepareStatement("DELETE FROM rewards WHERE player_uuid = ?");
			statement.setString(1, uuid);
			statement.executeUpdate();
			statement.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
}