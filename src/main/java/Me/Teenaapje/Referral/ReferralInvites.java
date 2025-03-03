package Me.Teenaapje.Referral;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import Me.Teenaapje.Referral.Utils.Utils;
import net.md_5.bungee.api.ChatColor;
import net.md_5.bungee.api.chat.ClickEvent;
import net.md_5.bungee.api.chat.TextComponent;


public class ReferralInvites {
	// list of invites
	ArrayList<Refer> referInvites;
	// the core
	ReferralCore core = ReferralCore.core;

	// init ReferralInvites
	public ReferralInvites () {
		referInvites = new ArrayList<Refer>();
	}

	// add and send invite
	public boolean AddToList(String to, String from) {
		Player fromPlayer = core.GetPlayer(from);

		if (fromPlayer == null) {
			// The referring player must be online
			return false;
		}

		// check if already exists
		if (IsInList(to, from)) {
			Utils.SendMessage(fromPlayer, core.config.alreadySendRef);
			return false;
		}

		// Get UUID of the player being referred
		UUID toPlayerUUID = core.GetPlayerUUID(to);

		// If we couldn't find the player's UUID, they've never played on the server
		if (toPlayerUUID == null) {
			Utils.SendMessage(fromPlayer, core.config.playerNeverPlayed.replace("%player_name%", to));
			return false;
		}

		// Check if it is the same player
		if (fromPlayer.getUniqueId().equals(toPlayerUUID)) {
			Utils.SendMessage(fromPlayer, core.config.alreadySendRef);
			return false;
		}

		// Add to list - store with UUID to ensure consistency
		referInvites.add(new Refer(to, from, toPlayerUUID, fromPlayer.getUniqueId()));

		// Get the player being referred (might be null if offline)
		Player toPlayer = core.GetPlayer(to);

		// If the player is online, send them the invitation immediately
		if (toPlayer != null) {
			// get the buttons
			TextComponent accept = Utils.CreateTextComponent(core.config.accept, ChatColor.GREEN, true, ClickEvent.Action.RUN_COMMAND, "/ref accept " + from);
			TextComponent decline = Utils.CreateTextComponent(core.config.decline, ChatColor.RED, true, ClickEvent.Action.RUN_COMMAND, "/ref reject " + from);

			// send invite
			Utils.SendMessage(toPlayer, core.config.youGotRefer, fromPlayer);
			toPlayer.spigot().sendMessage(accept, decline);

			// notify that it has been sent
			Utils.SendMessage(fromPlayer, core.config.youSendRequest, toPlayer);
		} else {
			// Player is offline, notify the referring player
			String playerName = core.GetPlayerName(toPlayerUUID);
			if (playerName == null) playerName = to; // Fallback to the provided name if GetPlayerName returns null

			Utils.SendMessage(fromPlayer, core.config.offlineReferralSent.replace("%player_name%", playerName));
		}

		return true;
	}

	// remove from list
	public boolean RemoveFromList(String ref, String refer) {
		Iterator<Refer> itr = referInvites.iterator();
		while(itr.hasNext()){
			Refer st = (Refer)itr.next();
			if (st.ref.contains(ref) && st.refer.contains(refer)) {
				itr.remove();
				return true;
			}
		}

		return false;
	}

	// is in list
	public boolean IsInList(String ref, String refer) {
		Iterator<Refer> itr = referInvites.iterator();
		while(itr.hasNext()){
			Refer st = (Refer)itr.next();
			if (st.ref.toLowerCase().compareTo(ref.toLowerCase()) == 0 && st.refer.toLowerCase().compareTo(refer.toLowerCase()) == 0) {
				return true;
			}
		}
		return false;
	}

	// Get all pending invites for a player
	public ArrayList<Refer> GetPendingInvites(String playerName) {
		ArrayList<Refer> pendingInvites = new ArrayList<>();
		Iterator<Refer> itr = referInvites.iterator();

		while(itr.hasNext()) {
			Refer invite = itr.next();
			if (invite.ref.equalsIgnoreCase(playerName)) {
				pendingInvites.add(invite);
			}
		}

		return pendingInvites;
	}
}


class Refer {
	String ref;
	String refer;
	UUID refUUID;
	UUID referUUID;

	Refer(String _ref, String _refer) {
		this.ref = _ref;
		this.refer = _refer;
		this.refUUID = null;
		this.referUUID = null;
	}

	Refer(String _ref, String _refer, UUID _refUUID, UUID _referUUID) {
		this.ref = _ref;
		this.refer = _refer;
		this.refUUID = _refUUID;
		this.referUUID = _referUUID;
	}
}