package Me.Teenaapje.Referral.Utils;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import net.md_5.bungee.api.ChatColor;
import net.md_5.bungee.api.chat.ClickEvent;
import net.md_5.bungee.api.chat.TextComponent;
import me.clip.placeholderapi.PlaceholderAPI;
import Me.Teenaapje.Referral.ReferralCore;

public class Utils {	
	// send message to player
	public static boolean SendMessage(CommandSender sendTo, String text) {
		return SendMessage(sendTo, text, null);
	}
	public static boolean SendMessage(CommandSender sendTo, String text, Player playerPlaceholder) {
		if (sendTo == null || text == null) {
			return false;
		}
		
		try {
			// get placeholder of other player
			if (playerPlaceholder != null) {
				text = SetPlaceHolders(playerPlaceholder, text);
			}
			
			// colorcode and send
			sendTo.sendMessage(ColorCode(text));
			
			return true;
		} catch (Exception e) {
			ReferralCore.core.getLogger().severe("Error sending message: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean SendMessage(Player player, String text) {
		return SendMessage(player, text, null);
	}
	public static boolean SendMessage(Player player, String text, Player playerPlaceholder) {
		if (player == null || text == null) {
			return false;
		}
		
		try {
			// get placeholder of other player
			if (playerPlaceholder != null) {
				text = SetPlaceHolders(playerPlaceholder, text);
			} else {
				text = SetPlaceHolders(player, text);			
			}
			
			// colorcode and send
			player.sendMessage(ColorCode(text));
			
			return true;
		} catch (Exception e) {
			ReferralCore.core.getLogger().severe("Error sending message to player: " + e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
	public static String SetPlaceHolders(Player player, String text) {
		if (player == null || text == null) {
			return text;
		}
		
		try {
			if (ConfigManager.placeholderAPIEnabled) {
				return PlaceholderAPI.setPlaceholders(player, text);
			}
			
			// Replace placeholders with their values
			String uuid = player.getUniqueId().toString();
			String name = player.getName();
			
			return text.replace("%referral_total%", Integer.toString(ReferralCore.core.db.GetReferrals(uuid, name)))
					   .replace("%referral_refed%", String.valueOf(ReferralCore.core.db.PlayerReferrald(uuid, name)))
					   .replace("%referral_referred_by%", ReferralCore.core.db.PlayerReferraldByName(uuid))
					   .replace("%referral_is_referred%", ReferralCore.core.db.PlayerReferrald(uuid, name) ? "Yes" : "No")
					   .replace("%player_name%", name);
		} catch (Exception e) {
			ReferralCore.core.getLogger().severe("Error setting placeholders: " + e.getMessage());
			e.printStackTrace();
			return text;
		}
	}
	
	/// Color code text
	public static String ColorCode(String text) {
		if (text == null) {
			return "";
		}
		return ChatColor.translateAlternateColorCodes('&', text);
	}
	
	// create text component
	public static TextComponent CreateTextComponent(String text, ChatColor color, boolean bold, ClickEvent.Action action, String runCommand) {
		if (text == null) {
			text = "";
		}
		if (runCommand == null) {
			runCommand = "";
		}
		
		TextComponent component = new TextComponent(text);
		component.setColor(color);
		component.setBold(bold);
		component.setClickEvent(new ClickEvent(action, runCommand));
		return component;
	}
	
	// check if its the same player
	public static boolean IsPlayerSelf(Player player, String name) {
		if (player == null || name == null) {
			return false;
		}
		return player.getName().toLowerCase().compareTo(name.toLowerCase()) == 0;
	}
	public static boolean IsPlayerSelf(Player a, Player b) {
		if (a == null || b == null) {
			return false;
		}
		return a.getUniqueId().equals(b.getUniqueId());
	}
	
	public static boolean IsConsole(CommandSender sender) {
		return !(sender instanceof Player);
	}
	
	public static void Console(String text) {
		if (text == null) {
			return;
		}
		ReferralCore.core.getLogger().info(ChatColor.translateAlternateColorCodes('&', text));
	}
}