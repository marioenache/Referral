package Me.Teenaapje.Referral;

import java.util.List;
import java.util.UUID;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import Me.Teenaapje.Referral.Commands.CommandManager;
import Me.Teenaapje.Referral.Database.Database;
import Me.Teenaapje.Referral.PlaceHolders.PlaceHolders;
import Me.Teenaapje.Referral.Utils.ConfigManager;
import Me.Teenaapje.Referral.Utils.Utils;

public class ReferralCore extends JavaPlugin{
	public static ReferralCore core;

	public ConfigManager config;
	public ReferralInvites rInvites;
	public ReferralMilestone milestone;
	public Database db;

	public void onEnable() {
		saveDefaultConfig();
		// set the plugin
		ReferralCore.core = this;

		// get the config
		config 		= new ConfigManager();
		rInvites 	= new ReferralInvites();
		milestone 	= new ReferralMilestone();
		db 			= new Database();

		// set placeholders if papi is there
		if (ConfigManager.placeholderAPIEnabled) {
			new PlaceHolders().register();
		}

		new CommandManager();

		new ReferralEvents();

		Utils.Console("[Referrel] Initialized - Enjoy");
	}

	public void onDisable() {
		db.CloseConnection();
	}


	@SuppressWarnings("deprecation")
	public Player GetPlayer(String name) {
		Player player = this.getServer().getPlayer(name);
		if (player != null) {
			return player;
		}

		return getServer().getOfflinePlayer(name).getPlayer();
	}

	/**
	 * Get a player's UUID by name, works for both online and offline players
	 * @param name The player's name
	 * @return The UUID of the player, or null if not found
	 */
	public UUID GetPlayerUUID(String name) {
		// First check online players
		Player player = this.getServer().getPlayer(name);
		if (player != null) {
			return player.getUniqueId();
		}

		// Then check offline players by exact name match
		OfflinePlayer[] offlinePlayers = Bukkit.getOfflinePlayers();
		for (OfflinePlayer offlinePlayer : offlinePlayers) {
			if (offlinePlayer.getName() != null && offlinePlayer.getName().equalsIgnoreCase(name)) {
				return offlinePlayer.getUniqueId();
			}
		}

		// As a fallback, try the standard method
		OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(name);
		if (offlinePlayer != null && offlinePlayer.hasPlayedBefore()) {
			return offlinePlayer.getUniqueId();
		}

		return null;
	}

	/**
	 * Get a player's name by UUID
	 * @param uuid The player's UUID
	 * @return The name of the player, or null if not found
	 */
	public String GetPlayerName(UUID uuid) {
		// First check online players
		Player player = this.getServer().getPlayer(uuid);
		if (player != null) {
			return player.getName();
		}

		// Then check offline players
		OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);
		if (offlinePlayer != null && offlinePlayer.getName() != null) {
			return offlinePlayer.getName();
		}

		return null;
	}

	public void UseCommands(List<?> commands, Player player) {
		for (int i = 0; i < commands.size(); i++) {
			String command = (String) commands.get(i);

			getServer().getScheduler().runTask(this, new Runnable() {
				@Override
				public void run() {
					getServer().dispatchCommand(getServer().getConsoleSender(), command.replace("<player>", player.getName()));
				}
			});
		}
	}

}